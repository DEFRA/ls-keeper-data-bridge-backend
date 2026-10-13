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
    /// <summary>How long a run is trusted to still be alive after its last sign of progress.</summary>
    public static readonly TimeSpan LeaseDuration = EtlImportProgress.LeaseDuration;

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
        var document = EtlImportProgress.Queued(importId, sourceType, dataset, UtcNow);

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

    public async Task MarkStageRunningAsync(Guid importId, string stageName, CancellationToken cancellationToken)
    {
        var now = UtcNow;
        var update = Builders<EtlImportDocument>.Update
            .Set(d => d.CurrentStage, stageName)
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

        EtlImportProgress.ApplyStage(document, progress, UtcNow);

        await ReplaceAsync(document, cancellationToken);
    }

    public Task MarkSucceededAsync(Guid importId, CancellationToken cancellationToken)
        => CompleteAsync(importId, EtlImportStatus.Succeeded, error: null, detail: null, cancellationToken);

    public Task MarkFailedAsync(Guid importId, string error, EtlImportErrorDetail? detail, CancellationToken cancellationToken)
        => CompleteAsync(importId, EtlImportStatus.Failed, error, detail, cancellationToken);

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

        return candidates.FirstOrDefault(d => EtlImportProgress.IsInFlight(d, UtcNow));
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

    public async Task RecordPurgeAsync(EtlPurgeRecord purge, CancellationToken cancellationToken)
    {
        var document = EtlImportProgress.Purged(purge, UtcNow);

        await _imports.ReplaceOneAsync(
            d => d.ImportId == purge.PurgeId,
            document,
            new ReplaceOptions { IsUpsert = true },
            cancellationToken);
    }

    private async Task CompleteAsync(Guid importId, EtlImportStatus status, string? error, EtlImportErrorDetail? detail, CancellationToken cancellationToken)
    {
        var update = Builders<EtlImportDocument>.Update
            .Set(d => d.Status, status.ToString())
            .Set(d => d.CompletedAtUtc, UtcNow)
            .Set(d => d.CurrentStage, null)
            .Set(d => d.LeaseExpiresAtUtc, null)
            .Set(d => d.Error, error)
            .Set(d => d.ErrorDetail, detail);

        await _imports.UpdateOneAsync(d => d.ImportId == importId, update, cancellationToken: cancellationToken);
    }

    private Task<EtlImportDocument?> LoadAsync(Guid importId, CancellationToken cancellationToken)
        => _imports.Find(d => d.ImportId == importId).FirstOrDefaultAsync(cancellationToken)!;

    private Task ReplaceAsync(EtlImportDocument document, CancellationToken cancellationToken)
        => _imports.ReplaceOneAsync(d => d.ImportId == document.ImportId, document, cancellationToken: cancellationToken);

    private EtlImportDocument AsAbandonedIfLapsed(EtlImportDocument document)
        => EtlImportProgress.AsAbandonedIfLapsed(document, UtcNow);

    private DateTime UtcNow => _timeProvider.GetUtcNow().UtcDateTime;
}
