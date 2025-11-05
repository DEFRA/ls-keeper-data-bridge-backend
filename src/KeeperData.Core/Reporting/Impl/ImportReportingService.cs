using KeeperData.Core.Database;
using KeeperData.Core.Reporting.Domain;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Reporting.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KeeperData.Core.Reporting.Impl;

public class ImportReportingService : IImportReportingService
{
    private readonly IMongoCollection<ImportReportDocument> _importReports;
    private readonly IMongoCollection<ImportFileDocument> _importFiles;
    private readonly IMongoCollection<RecordLineageDocument> _recordLineage;
    private readonly IMongoCollection<LineageEventDocument> _recordLineageEvents;
    private readonly ILineageIdGenerator _idGenerator;
    private readonly ILineageMapper _mapper;
    private readonly ILineageIndexManager _indexManager;
    private readonly ILogger<ImportReportingService> _logger;

    public ImportReportingService(
        IMongoClient mongoClient,
        IOptions<IDatabaseConfig> databaseConfig,
        ILineageIdGenerator idGenerator,
        ILineageMapper mapper,
        ILineageIndexManagerFactory indexManagerFactory,
        ILogger<ImportReportingService> logger)
    {
        var database = mongoClient.GetDatabase(databaseConfig.Value.DatabaseName);
        _importReports = database.GetCollection<ImportReportDocument>("import_reports");
        _importFiles = database.GetCollection<ImportFileDocument>("import_files");
        _recordLineage = database.GetCollection<RecordLineageDocument>("record_lineage");
        _recordLineageEvents = database.GetCollection<LineageEventDocument>("record_lineage_events");
        _idGenerator = idGenerator ?? throw new ArgumentNullException(nameof(idGenerator));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _indexManager = indexManagerFactory?.Create(_recordLineageEvents) ?? throw new ArgumentNullException(nameof(indexManagerFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<ImportReport> StartImportAsync(Guid importId, string sourceType, CancellationToken ct)
    {
        _logger.LogInformation("Starting import report for ImportId: {ImportId}, SourceType: {SourceType}", importId, sourceType);

        var document = new ImportReportDocument
        {
            ImportId = importId,
            SourceType = sourceType,
            Status = ImportStatus.Started.ToString(),
            StartedAtUtc = DateTime.UtcNow,
            AcquisitionPhase = new AcquisitionPhaseDocument
            {
                Status = PhaseStatus.NotStarted.ToString(),
                FilesDiscovered = 0,
                FilesProcessed = 0,
                FilesFailed = 0
            },
            IngestionPhase = new IngestionPhaseDocument
            {
                Status = PhaseStatus.NotStarted.ToString(),
                FilesProcessed = 0,
                RecordsCreated = 0,
                RecordsUpdated = 0,
                RecordsDeleted = 0
            }
        };

        await _importReports.InsertOneAsync(document, cancellationToken: ct);

        return MapToImportReport(document);
    }

    public async Task UpdateAcquisitionPhaseAsync(Guid importId, AcquisitionPhaseUpdate update, CancellationToken ct)
    {
        _logger.LogDebug("Updating acquisition phase for ImportId: {ImportId}", importId);

        var filter = Builders<ImportReportDocument>.Filter.Eq(x => x.ImportId, importId);

        // First, check if StartedAtUtc needs to be set
        bool needsStartTime = false;
        if (update.Status == PhaseStatus.Started)
        {
            var currentDoc = await _importReports.Find(filter).FirstOrDefaultAsync(ct);
            needsStartTime = currentDoc?.AcquisitionPhase?.StartedAtUtc == null;
        }

        var updateBuilder = Builders<ImportReportDocument>.Update;

        var updates = new List<UpdateDefinition<ImportReportDocument>>
        {
            updateBuilder.Set("AcquisitionPhase.Status", update.Status.ToString()),
            updateBuilder.Set("AcquisitionPhase.FilesDiscovered", update.FilesDiscovered),
            updateBuilder.Set("AcquisitionPhase.FilesProcessed", update.FilesProcessed),
            updateBuilder.Set("AcquisitionPhase.FilesFailed", update.FilesFailed)
        };

        if (needsStartTime)
        {
            updates.Add(updateBuilder.Set("AcquisitionPhase.StartedAtUtc", DateTime.UtcNow));
        }

        if (update.CompletedAtUtc.HasValue)
        {
            updates.Add(updateBuilder.Set("AcquisitionPhase.CompletedAtUtc", update.CompletedAtUtc.Value));
        }

        var combinedUpdate = updateBuilder.Combine(updates);
        await _importReports.UpdateOneAsync(filter, combinedUpdate, cancellationToken: ct);
    }

    public async Task UpdateIngestionPhaseAsync(Guid importId, IngestionPhaseUpdate update, CancellationToken ct)
    {
        _logger.LogDebug("Updating ingestion phase for ImportId: {ImportId}", importId);

        var filter = Builders<ImportReportDocument>.Filter.Eq(x => x.ImportId, importId);

        // First, check if StartedAtUtc needs to be set
        bool needsStartTime = false;
        if (update.Status == PhaseStatus.Started)
        {
            var currentDoc = await _importReports.Find(filter).FirstOrDefaultAsync(ct);
            needsStartTime = currentDoc?.IngestionPhase?.StartedAtUtc == null;
        }

        var updateBuilder = Builders<ImportReportDocument>.Update;

        var updates = new List<UpdateDefinition<ImportReportDocument>>
        {
            updateBuilder.Set("IngestionPhase.Status", update.Status.ToString()),
            updateBuilder.Set("IngestionPhase.FilesProcessed", update.FilesProcessed),
            updateBuilder.Set("IngestionPhase.RecordsCreated", update.RecordsCreated),
            updateBuilder.Set("IngestionPhase.RecordsUpdated", update.RecordsUpdated),
            updateBuilder.Set("IngestionPhase.RecordsDeleted", update.RecordsDeleted)
        };

        if (needsStartTime)
        {
            updates.Add(updateBuilder.Set("IngestionPhase.StartedAtUtc", DateTime.UtcNow));
        }

        if (update.CompletedAtUtc.HasValue)
        {
            updates.Add(updateBuilder.Set("IngestionPhase.CompletedAtUtc", update.CompletedAtUtc.Value));
        }

        // Update CurrentFileStatus if provided
        if (update.CurrentFileStatus != null)
        {
            var statusDoc = new IngestionCurrentFileStatusDocument
            {
                FileName = update.CurrentFileStatus.FileName,
                TotalRows = update.CurrentFileStatus.TotalRows,
                RowNumber = update.CurrentFileStatus.RowNumber,
                PercentageCompleted = update.CurrentFileStatus.PercentageCompleted,
                RowsPerMinute = update.CurrentFileStatus.RowsPerMinute,
                EstimatedTimeRemaining = update.CurrentFileStatus.EstimatedTimeRemaining,
                EstimatedCompletionUtc = update.CurrentFileStatus.EstimatedCompletionUtc
            };
            updates.Add(updateBuilder.Set("IngestionPhase.CurrentFileStatus", statusDoc));
        }
        else
        {
            // Clear CurrentFileStatus if null is provided
            updates.Add(updateBuilder.Unset("IngestionPhase.CurrentFileStatus"));
        }

        var combinedUpdate = updateBuilder.Combine(updates);
        await _importReports.UpdateOneAsync(filter, combinedUpdate, cancellationToken: ct);
    }

    public async Task CompleteImportAsync(Guid importId, ImportStatus status, string? error, CancellationToken ct)
    {
        _logger.LogInformation("Completing import report for ImportId: {ImportId}, Status: {Status}", importId, status);

        var filter = Builders<ImportReportDocument>.Filter.Eq(x => x.ImportId, importId);
        var updateBuilder = Builders<ImportReportDocument>.Update
            .Set(x => x.Status, status.ToString())
            .Set(x => x.CompletedAtUtc, DateTime.UtcNow);

        if (!string.IsNullOrWhiteSpace(error))
        {
            updateBuilder = updateBuilder.Set(x => x.Error, error);
        }

        await _importReports.UpdateOneAsync(filter, updateBuilder, cancellationToken: ct);
    }

    public async Task<bool> IsFileProcessedAsync(string fileKey, string md5Hash, CancellationToken ct)
    {
        var filter = Builders<ImportFileDocument>.Filter.And(
            Builders<ImportFileDocument>.Filter.Eq(x => x.FileKey, fileKey),
            Builders<ImportFileDocument>.Filter.Eq(x => x.Md5Hash, md5Hash),
            Builders<ImportFileDocument>.Filter.In(x => x.Status,
                new[] { FileProcessingStatus.Acquired.ToString(), FileProcessingStatus.Ingested.ToString() })
        );

        var count = await _importFiles.CountDocumentsAsync(filter, cancellationToken: ct);
        return count > 0;
    }

    public async Task RecordFileAcquisitionAsync(Guid importId, FileAcquisitionRecord record, CancellationToken ct)
    {
        _logger.LogDebug("Recording file acquisition for ImportId: {ImportId}, FileKey: {FileKey}", importId, record.FileKey);

        var document = new ImportFileDocument
        {
            ImportId = importId,
            FileName = record.FileName,
            FileKey = record.FileKey,
            DatasetName = record.DatasetName,
            Md5Hash = record.Md5Hash,
            FileSize = record.FileSize,
            Status = record.Status.ToString(),
            AcquisitionDetails = new FileAcquisitionDetailsDocument
            {
                AcquiredAtUtc = record.AcquiredAtUtc,
                SourceKey = record.SourceKey,
                DecryptionDurationMs = record.DecryptionDurationMs
            },
            Error = record.Error
        };

        await _importFiles.InsertOneAsync(document, cancellationToken: ct);
    }

    public async Task RecordFileIngestionAsync(Guid importId, FileIngestionRecord record, CancellationToken ct)
    {
        _logger.LogDebug("Recording file ingestion for ImportId: {ImportId}, FileKey: {FileKey}", importId, record.FileKey);

        var filter = Builders<ImportFileDocument>.Filter.And(
            Builders<ImportFileDocument>.Filter.Eq(x => x.ImportId, importId),
            Builders<ImportFileDocument>.Filter.Eq(x => x.FileKey, record.FileKey)
        );

        var update = Builders<ImportFileDocument>.Update
            .Set(x => x.Status, record.Status.ToString())
            .Set(x => x.IngestionDetails, new FileIngestionDetailsDocument
            {
                IngestedAtUtc = record.IngestedAtUtc,
                RecordsProcessed = record.RecordsProcessed,
                RecordsCreated = record.RecordsCreated,
                RecordsUpdated = record.RecordsUpdated,
                RecordsDeleted = record.RecordsDeleted,
                IngestionDurationMs = record.IngestionDurationMs
            });

        if (!string.IsNullOrWhiteSpace(record.Error))
        {
            update = update.Set(x => x.Error, record.Error);
        }

        await _importFiles.UpdateOneAsync(filter, update, cancellationToken: ct);
    }

    public async Task RecordLineageEventAsync(RecordLineageEvent lineageEvent, CancellationToken ct)
    {
        await RecordLineageEventsBatchAsync(new[] { lineageEvent }, ct);
    }

    public async Task RecordLineageEventsBatchAsync(IEnumerable<RecordLineageEvent> lineageEvents, CancellationToken ct)
    {
        var eventsList = lineageEvents.ToList();
        if (eventsList.Count == 0)
        {
            return;
        }

        _logger.LogDebug("Recording {Count} lineage events", eventsList.Count);

        // Ensure indexes exist (idempotent, fast after first call)
        await _indexManager.EnsureIndexesAsync(ct);

        // Group events by lineage document ID for efficient parent upserts
        var groupedEvents = eventsList.GroupBy(e =>
            _idGenerator.GenerateLineageDocumentId(e.CollectionName, e.RecordId));

        var parentUpserts = new List<WriteModel<RecordLineageDocument>>();
        var eventInserts = new List<LineageEventDocument>();

        foreach (var group in groupedEvents)
        {
            var lineageDocId = group.Key;
            var latestEvent = group.OrderByDescending(e => e.EventDateUtc).First();

            // Prepare parent document upsert
            parentUpserts.Add(CreateParentUpsert(lineageDocId, latestEvent));

            // Prepare event documents
            foreach (var evt in group)
            {
                var eventId = _idGenerator.GenerateLineageEventId(
                    evt.CollectionName,
                    evt.RecordId,
                    evt.EventDateUtc);

                eventInserts.Add(_mapper.MapToEventDocument(evt, eventId, lineageDocId));
            }
        }

        // Execute bulk operations
        await ExecuteBulkOperationsAsync(parentUpserts, eventInserts, ct);
    }

    private WriteModel<RecordLineageDocument> CreateParentUpsert(
        string lineageDocId,
        RecordLineageEvent latestEvent)
    {
        var filter = Builders<RecordLineageDocument>.Filter.Eq(x => x.Id, lineageDocId);
        var currentStatus = latestEvent.EventType == RecordEventType.Deleted ? "Deleted" : "Active";

        var update = Builders<RecordLineageDocument>.Update
            .Set(x => x.CurrentStatus, currentStatus)
            .Set(x => x.LastModifiedByImport, latestEvent.ImportId)
            .Set(x => x.LastModifiedAtUtc, latestEvent.EventDateUtc)
            .SetOnInsert(x => x.Id, lineageDocId)
            .SetOnInsert(x => x.RecordId, latestEvent.RecordId)
            .SetOnInsert(x => x.CollectionName, latestEvent.CollectionName)
            .SetOnInsert(x => x.CreatedByImport, latestEvent.ImportId)
            .SetOnInsert(x => x.CreatedAtUtc, latestEvent.EventDateUtc);

        return new UpdateOneModel<RecordLineageDocument>(filter, update) { IsUpsert = true };
    }

    private async Task ExecuteBulkOperationsAsync(
        List<WriteModel<RecordLineageDocument>> parentUpserts,
        List<LineageEventDocument> eventInserts,
        CancellationToken ct)
    {
        // Execute parent upserts
        if (parentUpserts.Count > 0)
        {
            await _recordLineage.BulkWriteAsync(
                parentUpserts,
                new BulkWriteOptions { IsOrdered = false },
                ct);
        }

        // Execute event inserts
        if (eventInserts.Count > 0)
        {
            await _recordLineageEvents.InsertManyAsync(
                eventInserts,
                new InsertManyOptions { IsOrdered = false },
                ct);
        }
    }

    public async Task<ImportReport?> GetImportReportAsync(Guid importId, CancellationToken ct)
    {
        var filter = Builders<ImportReportDocument>.Filter.Eq(x => x.ImportId, importId);
        var document = await _importReports.Find(filter).FirstOrDefaultAsync(ct);

        return document != null ? MapToImportReport(document) : null;
    }

    public async Task<IReadOnlyList<FileProcessingReport>> GetFileReportsAsync(Guid importId, CancellationToken ct)
    {
        var filter = Builders<ImportFileDocument>.Filter.Eq(x => x.ImportId, importId);
        var documents = await _importFiles.Find(filter).ToListAsync(ct);

        return documents.Select(MapToFileProcessingReport).ToList();
    }

    public async Task<RecordLifecycle?> GetRecordLifecycleAsync(string collectionName, string recordId, CancellationToken ct)
    {
        var lineageDocId = _idGenerator.GenerateLineageDocumentId(collectionName, recordId);

        // Get parent document (O(1) direct lookup by composite _id)
        var parentFilter = Builders<RecordLineageDocument>.Filter.Eq(x => x.Id, lineageDocId);
        var parentDoc = await _recordLineage.Find(parentFilter).FirstOrDefaultAsync(ct);

        if (parentDoc == null) return null;

        // Get all events for this record (efficiently indexed query)
        var eventsFilter = Builders<LineageEventDocument>.Filter.Eq(x => x.LineageDocumentId, lineageDocId);
        var eventsSort = Builders<LineageEventDocument>.Sort.Ascending(x => x.Id); // Chronological by design
        var events = await _recordLineageEvents
            .Find(eventsFilter)
            .Sort(eventsSort)
            .ToListAsync(ct);

        return _mapper.MapToRecordLifecycle(parentDoc, events);
    }

    public async Task<IReadOnlyList<RecordLineageEvent>> GetRecordLineageAsync(string collectionName, string recordId, CancellationToken ct)
    {
        var lifecycle = await GetRecordLifecycleAsync(collectionName, recordId, ct);
        return lifecycle?.Events ?? Array.Empty<RecordLineageEvent>();
    }

    public async Task<IReadOnlyList<ImportSummary>> GetImportSummariesAsync(int skip = 0, int top = 10, CancellationToken ct = default)
    {
        _logger.LogDebug("Getting import summaries with skip: {Skip}, top: {Top}", skip, top);

        var documents = await _importReports
            .Find(Builders<ImportReportDocument>.Filter.Empty)
            .SortByDescending(x => x.StartedAtUtc)
            .Skip(skip)
            .Limit(top)
            .ToListAsync(ct);

        return documents.Select(MapToImportSummary).ToList();
    }

    private static ImportReport MapToImportReport(ImportReportDocument doc)
    {
        return new ImportReport
        {
            ImportId = doc.ImportId,
            SourceType = doc.SourceType,
            Status = Enum.Parse<ImportStatus>(doc.Status),
            StartedAtUtc = doc.StartedAtUtc,
            CompletedAtUtc = doc.CompletedAtUtc,
            AcquisitionPhase = doc.AcquisitionPhase != null ? new AcquisitionPhaseReport
            {
                Status = Enum.Parse<PhaseStatus>(doc.AcquisitionPhase.Status),
                FilesDiscovered = doc.AcquisitionPhase.FilesDiscovered,
                FilesProcessed = doc.AcquisitionPhase.FilesProcessed,
                FilesFailed = doc.AcquisitionPhase.FilesFailed,
                StartedAtUtc = doc.AcquisitionPhase.StartedAtUtc,
                CompletedAtUtc = doc.AcquisitionPhase.CompletedAtUtc
            } : null,
            IngestionPhase = doc.IngestionPhase != null ? new IngestionPhaseReport
            {
                Status = Enum.Parse<PhaseStatus>(doc.IngestionPhase.Status),
                FilesProcessed = doc.IngestionPhase.FilesProcessed,
                RecordsCreated = doc.IngestionPhase.RecordsCreated,
                RecordsUpdated = doc.IngestionPhase.RecordsUpdated,
                RecordsDeleted = doc.IngestionPhase.RecordsDeleted,
                StartedAtUtc = doc.IngestionPhase.StartedAtUtc,
                CompletedAtUtc = doc.IngestionPhase.CompletedAtUtc,
                CurrentFileStatus = doc.IngestionPhase.CurrentFileStatus != null ? new IngestionCurrentFileStatus
                {
                    FileName = doc.IngestionPhase.CurrentFileStatus.FileName,
                    TotalRows = doc.IngestionPhase.CurrentFileStatus.TotalRows,
                    RowNumber = doc.IngestionPhase.CurrentFileStatus.RowNumber,
                    PercentageCompleted = doc.IngestionPhase.CurrentFileStatus.PercentageCompleted,
                    RowsPerMinute = doc.IngestionPhase.CurrentFileStatus.RowsPerMinute,
                    EstimatedTimeRemaining = doc.IngestionPhase.CurrentFileStatus.EstimatedTimeRemaining,
                    EstimatedCompletionUtc = doc.IngestionPhase.CurrentFileStatus.EstimatedCompletionUtc
                } : null
            } : null,
            Error = doc.Error
        };
    }

    private static FileProcessingReport MapToFileProcessingReport(ImportFileDocument doc)
    {
        return new FileProcessingReport
        {
            ImportId = doc.ImportId,
            FileName = doc.FileName,
            FileKey = doc.FileKey,
            DatasetName = doc.DatasetName,
            Md5Hash = doc.Md5Hash,
            FileSize = doc.FileSize,
            Status = Enum.Parse<FileProcessingStatus>(doc.Status),
            Acquisition = doc.AcquisitionDetails != null ? new AcquisitionDetails
            {
                AcquiredAtUtc = doc.AcquisitionDetails.AcquiredAtUtc,
                SourceKey = doc.AcquisitionDetails.SourceKey,
                DecryptionDurationMs = doc.AcquisitionDetails.DecryptionDurationMs
            } : null,
            Ingestion = doc.IngestionDetails != null ? new IngestionDetails
            {
                IngestedAtUtc = doc.IngestionDetails.IngestedAtUtc,
                RecordsProcessed = doc.IngestionDetails.RecordsProcessed,
                RecordsCreated = doc.IngestionDetails.RecordsCreated,
                RecordsUpdated = doc.IngestionDetails.RecordsUpdated,
                RecordsDeleted = doc.IngestionDetails.RecordsDeleted,
                IngestionDurationMs = doc.IngestionDetails.IngestionDurationMs
            } : null,
            Error = doc.Error
        };
    }

    private static ImportSummary MapToImportSummary(ImportReportDocument doc)
    {
        return new ImportSummary
        {
            ImportId = doc.ImportId,
            Status = Enum.Parse<ImportStatus>(doc.Status),
            StartedAtUtc = doc.StartedAtUtc,
            CompletedAtUtc = doc.CompletedAtUtc,
            FilesProcessed = doc.IngestionPhase?.FilesProcessed ?? 0,
            RecordsCreated = doc.IngestionPhase?.RecordsCreated ?? 0,
            RecordsUpdated = doc.IngestionPhase?.RecordsUpdated ?? 0,
            RecordsDeleted = doc.IngestionPhase?.RecordsDeleted ?? 0
        };
    }
}