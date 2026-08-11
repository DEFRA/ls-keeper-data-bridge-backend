using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Database;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KeeperData.Core.EtlPipeline.Status;

/// <summary>Mongo-backed import status.
///
/// One run writes its own document and nothing else writes it, so progress is a read-merge-replace
/// rather than a set of field-level updates - the dataset entries have to be merged across stages,
/// and merging in memory keeps that logic readable.</summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB persistence - covered by integration tests.")]
public sealed class MongoEtlImportStatusStore : IEtlImportStatusStore
{
    /// <summary>How long a run is trusted to still be alive after its last sign of progress. Longer
    /// than any single stage is expected to take, since the lease is only extended between stages.</summary>
    public static readonly TimeSpan LeaseDuration = TimeSpan.FromMinutes(30);

    private readonly IMongoCollection<EtlImportDocument> _imports;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<MongoEtlImportStatusStore> _logger;

    public MongoEtlImportStatusStore(
        IMongoClient mongoClient,
        IOptions<IDatabaseConfig> databaseConfig,
        TimeProvider timeProvider,
        ILogger<MongoEtlImportStatusStore> logger)
    {
        var database = mongoClient.GetDatabase(databaseConfig.Value.DatabaseName);
        _imports = database.GetCollection<EtlImportDocument>("etl_pipeline_imports");
        _timeProvider = timeProvider;
        _logger = logger;
    }

    public async Task CreateQueuedAsync(Guid importId, string sourceType, string? dataset, CancellationToken cancellationToken)
    {
        var now = UtcNow;

        var document = new EtlImportDocument
        {
            ImportId = importId,
            Status = EtlImportStatus.Queued.ToString(),
            SourceType = sourceType,
            Dataset = dataset,
            RequestedAtUtc = now,
            LeaseExpiresAtUtc = now.Add(LeaseDuration)
        };

        await _imports.ReplaceOneAsync(
            d => d.ImportId == importId,
            document,
            new ReplaceOptions { IsUpsert = true },
            cancellationToken);
    }

    public async Task MarkRunningAsync(Guid importId, IReadOnlyList<string> stageNames, CancellationToken cancellationToken)
    {
        var now = UtcNow;

        var update = Builders<EtlImportDocument>.Update
            .Set(d => d.Status, EtlImportStatus.Running.ToString())
            .Set(d => d.StartedAtUtc, now)
            .Set(d => d.CurrentStage, stageNames.FirstOrDefault())
            .Set(d => d.LeaseExpiresAtUtc, now.Add(LeaseDuration));

        await _imports.UpdateOneAsync(d => d.ImportId == importId, update, cancellationToken: cancellationToken);
    }

    public async Task RecordStageAsync(Guid importId, EtlImportStageProgress progress, CancellationToken cancellationToken)
    {
        var document = await LoadAsync(importId, cancellationToken);

        if (document is null)
        {
            _logger.LogWarning("No import status document for {ImportId}; stage {Stage} not recorded", importId, progress.StageName);
            return;
        }

        var now = UtcNow;

        document.Stages.Add(new EtlImportStageDocument
        {
            Name = progress.StageName,
            ItemCount = progress.ItemCount,
            ElapsedMs = (long)progress.Elapsed.TotalMilliseconds,
            CompletedAtUtc = now
        });

        foreach (var dataset in progress.Datasets)
        {
            Merge(DatasetEntry(document, dataset.Dataset), dataset);
        }

        if (progress.DuckDbKey is not null)
        {
            document.DuckDbKey = progress.DuckDbKey;
        }

        document.CurrentStage = progress.StageName;
        document.LeaseExpiresAtUtc = now.Add(LeaseDuration);

        await ReplaceAsync(document, cancellationToken);
    }

    public Task MarkSucceededAsync(Guid importId, CancellationToken cancellationToken)
        => CompleteAsync(importId, EtlImportStatus.Succeeded, error: null, cancellationToken);

    public Task MarkFailedAsync(Guid importId, string error, CancellationToken cancellationToken)
        => CompleteAsync(importId, EtlImportStatus.Failed, error, cancellationToken);

    public async Task<EtlImportDocument?> GetAsync(Guid importId, CancellationToken cancellationToken)
    {
        var document = await LoadAsync(importId, cancellationToken);

        return document is null ? null : AsAbandonedIfLapsed(document);
    }

    public async Task<EtlImportDocument?> GetInFlightAsync(CancellationToken cancellationToken)
    {
        var active = new[] { EtlImportStatus.Queued.ToString(), EtlImportStatus.Running.ToString() };

        var candidates = await _imports
            .Find(d => active.Contains(d.Status))
            .SortByDescending(d => d.RequestedAtUtc)
            .Limit(10)
            .ToListAsync(cancellationToken);

        return candidates.FirstOrDefault(d => d.LeaseExpiresAtUtc is null || d.LeaseExpiresAtUtc > UtcNow);
    }

    public async Task<EtlImportPage> ListAsync(int skip, int top, CancellationToken cancellationToken)
    {
        var all = Builders<EtlImportDocument>.Filter.Empty;

        var documents = await _imports
            .Find(all)
            .SortByDescending(d => d.RequestedAtUtc)
            .Skip(skip)
            .Limit(top)
            .ToListAsync(cancellationToken);

        var totalCount = await _imports.CountDocumentsAsync(all, cancellationToken: cancellationToken);

        return new EtlImportPage([.. documents.Select(AsAbandonedIfLapsed)], totalCount);
    }

    private async Task CompleteAsync(Guid importId, EtlImportStatus status, string? error, CancellationToken cancellationToken)
    {
        var update = Builders<EtlImportDocument>.Update
            .Set(d => d.Status, status.ToString())
            .Set(d => d.CompletedAtUtc, UtcNow)
            .Set(d => d.CurrentStage, null)
            .Set(d => d.LeaseExpiresAtUtc, null)
            .Set(d => d.Error, error);

        await _imports.UpdateOneAsync(d => d.ImportId == importId, update, cancellationToken: cancellationToken);
    }

    private Task<EtlImportDocument?> LoadAsync(Guid importId, CancellationToken cancellationToken)
        => _imports.Find(d => d.ImportId == importId).FirstOrDefaultAsync(cancellationToken)!;

    private Task ReplaceAsync(EtlImportDocument document, CancellationToken cancellationToken)
        => _imports.ReplaceOneAsync(d => d.ImportId == document.ImportId, document, cancellationToken: cancellationToken);

    /// <summary>A run whose lease lapsed is not running - the process hosting it died. Reported as
    /// failed so a poller gets an answer instead of waiting forever on "Running".</summary>
    private EtlImportDocument AsAbandonedIfLapsed(EtlImportDocument document)
    {
        var running = document.Status is nameof(EtlImportStatus.Running) or nameof(EtlImportStatus.Queued);

        if (!running || document.LeaseExpiresAtUtc is null || document.LeaseExpiresAtUtc > UtcNow)
        {
            return document;
        }

        document.Status = EtlImportStatus.Failed.ToString();
        document.Error = "The run stopped reporting progress and is assumed to have been abandoned.";

        return document;
    }

    private static EtlImportDatasetDocument DatasetEntry(EtlImportDocument document, string dataset)
    {
        var existing = document.Datasets.Find(d => d.Dataset == dataset);

        if (existing is not null) return existing;

        var created = new EtlImportDatasetDocument { Dataset = dataset };
        document.Datasets.Add(created);

        return created;
    }

    private static void Merge(EtlImportDatasetDocument target, EtlImportDatasetProgress source)
    {
        if (source.SourceFiles.Count > 0)
        {
            target.SourceFiles = [.. source.SourceFiles.Select(f => new EtlImportSourceFileDocument { Key = f.Key, Size = f.Size })];
        }

        if (source.RawKeys.Count > 0) target.RawKeys = [.. source.RawKeys];
        if (source.NormalisedKeys.Count > 0) target.NormalisedKeys = [.. source.NormalisedKeys];

        target.SnapshotKey = source.SnapshotKey ?? target.SnapshotKey;
        target.SnapshotSourceTimestampUtc = source.SnapshotSourceTimestamp?.UtcDateTime ?? target.SnapshotSourceTimestampUtc;
        target.RowCount = source.RowCount ?? target.RowCount;
        target.RowsUpserted = source.RowsUpserted ?? target.RowsUpserted;
        target.RowsIgnoredDeletes = source.RowsIgnoredDeletes ?? target.RowsIgnoredDeletes;
    }

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;
}
