using CsvHelper;
using CsvHelper.Configuration;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;

namespace KeeperData.Core.ETL.Impl;

/// <summary>
/// This class is responsible for ingesting csv files into mongo.
/// </summary>
public class IngestionPipeline(
    IBlobStorageServiceFactory blobStorageServiceFactory,
    IExternalCatalogueServiceFactory ExternalCatalogueServiceFactory,
    IMongoClient mongoClient,
    IOptions<IDatabaseConfig> databaseConfig,
    IImportReportingService reportingService,
    CsvRowCounter csvRowCounter,
    ILogger<IngestionPipeline> logger) : IIngestionPipeline
{
    private const int BatchSize = 1000;
    private const int LogInterval = 100;
    private const int LineageEventBatchSize = 500;
    private readonly IDatabaseConfig _databaseConfig = databaseConfig.Value;
    private readonly CsvRowCounter _rowCounter = csvRowCounter;

    // MongoDB field name constants
    private const string FieldId = "_id";
    private const string FieldCreatedAtUtc = "CreatedAtUtc";
    private const string FieldUpdatedAtUtc = "UpdatedAtUtc";
    private const string FieldIsDeleted = "IsDeleted";
    private const string FieldDeletedAtUtc = "DeletedAtUtc";

    public async Task StartAsync(Guid importId, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        logger.LogInformation("Starting ingest pipeline for ImportId: {ImportId}", importId);

        try
        {
            var storageServices = InitializeStorageServices(importId);

            var fileSets = await DiscoverFilesAsync(importId, storageServices.CatalogueService, ct);

            await UpdateIngestionPhaseStartedAsync(importId, ct);

            var ingestionResults = await ProcessAllFilesAsync(
                importId,
                fileSets.FileSets,
                fileSets.TotalFiles,
                storageServices.BlobStorage,
                ct);

            await UpdateIngestionPhaseCompletedAsync(
                importId,
                ingestionResults.FilesProcessed,
                ingestionResults.RecordsCreated,
                ingestionResults.RecordsUpdated,
                ingestionResults.RecordsDeleted,
                ct);

            LogPipelineCompletion(importId, stopwatch);
        }
        catch (Exception ex)
        {
            LogPipelineFailure(importId, stopwatch, ex);
            throw;
        }
    }

    private (IBlobStorageService BlobStorage, ExternalCatalogueService CatalogueService) InitializeStorageServices(Guid importId)
    {
        var blobs = blobStorageServiceFactory.Get();
        var catalogueService = ExternalCatalogueServiceFactory.Create(blobs);

        logger.LogDebug("Initialized blob storage services for ImportId: {ImportId}", importId);

        return (blobs, catalogueService);
    }

    private async Task<(ImmutableList<FileSet> FileSets, int TotalFiles)> DiscoverFilesAsync(
        Guid importId,
        ExternalCatalogueService catalogueService,
        CancellationToken ct)
    {
        logger.LogInformation("Step 1: Discovering files for ImportId: {ImportId}", importId);

        var fileSets = await catalogueService.GetFileSetsAsync(20, ct);
        var totalFiles = fileSets.Sum(fs => fs.Files.Length);

        logger.LogInformation("Discovered {FileSetCount} file set(s) containing {TotalFileCount} file(s) for ImportId: {ImportId}",
            fileSets.Count,
            totalFiles,
            importId);

        return (fileSets, totalFiles);
    }

    private async Task UpdateIngestionPhaseStartedAsync(Guid importId, CancellationToken ct)
    {
        await reportingService.UpdateIngestionPhaseAsync(importId, new IngestionPhaseUpdate
        {
            Status = PhaseStatus.Started,
            FilesProcessed = 0,
            RecordsCreated = 0,
            RecordsUpdated = 0,
            RecordsDeleted = 0,
            CurrentFileStatus = null
        }, ct);
    }

    private async Task<IngestionTotals> ProcessAllFilesAsync(
        Guid importId,
        ImmutableList<FileSet> fileSets,
        int totalFiles,
        IBlobStorageService blobStorage,
        CancellationToken ct
    )
    {
        logger.LogInformation("Step 2: Processing and ingesting files for ImportId: {ImportId}", importId);

        var totals = new IngestionTotals();
        var processedFileCount = 0;

        foreach (var fileSet in fileSets)
        {
            logger.LogDebug("Processing file set for definition: {DefinitionName} with {FileCount} file(s) for ImportId: {ImportId}",
                fileSet.Definition.Name,
                fileSet.Files.Length,
                importId);

            foreach (var file in fileSet.Files)
            {
                processedFileCount++;

                var fileResult = await ProcessSingleFileAsync(
                    importId,
                    fileSet,
                    file,
                    processedFileCount,
                    totalFiles,
                    blobStorage,
                    ct);

                totals = totals.Add(fileResult);

                // Clear current file status after completion and update overall progress
                await UpdateIngestionPhaseProgressAsync(
                    importId,
                    processedFileCount,
                    totals,
                    null, // Clear current file status
                    ct);
            }
        }

        logger.LogInformation("Step 2 completed: Processed {ProcessedFileCount} file(s) for ImportId: {ImportId}",
            processedFileCount,
            importId);

        return totals with { FilesProcessed = processedFileCount };
    }

    private async Task<IngestionTotals> ProcessSingleFileAsync(
        Guid importId,
        FileSet fileSet,
        StorageObjectInfo file,
        int currentFileNumber,
        int totalFiles,
        IBlobStorageService blobStorage,
        CancellationToken ct)
    {
        var fileStopwatch = Stopwatch.StartNew();

        logger.LogInformation("Processing file {CurrentFile}/{TotalFiles}: {FileKey} for ImportId: {ImportId}",
            currentFileNumber,
            totalFiles,
            file.Key,
            importId);

        try
        {
            var fileMetrics = await IngestFileAsync(importId, blobStorage, fileSet, file, ct);
            fileStopwatch.Stop();

            await RecordSuccessfulIngestionAsync(importId, file, fileMetrics, fileStopwatch.ElapsedMilliseconds, ct);

            logger.LogInformation("Successfully ingested file: {FileKey} - Created: {Created}, Updated: {Updated}, Deleted: {Deleted}, Duration: {Duration}ms",
                file.Key,
                fileMetrics.RecordsCreated,
                fileMetrics.RecordsUpdated,
                fileMetrics.RecordsDeleted,
                fileStopwatch.ElapsedMilliseconds);

            return new IngestionTotals
            {
                RecordsCreated = fileMetrics.RecordsCreated,
                RecordsUpdated = fileMetrics.RecordsUpdated,
                RecordsDeleted = fileMetrics.RecordsDeleted
            };
        }
        catch (Exception ex)
        {
            fileStopwatch.Stop();
            logger.LogError(ex, "Failed to ingest file: {FileKey} after {Duration}ms for ImportId: {ImportId}",
                file.Key,
                fileStopwatch.ElapsedMilliseconds,
                importId);

            await RecordFailedIngestionAsync(importId, file, fileStopwatch.ElapsedMilliseconds, ex, ct);

            throw;
        }
    }

    private async Task<FileIngestionMetrics> IngestFileAsync(
        Guid importId,
        IBlobStorageService blobs,
        FileSet fileSet,
        StorageObjectInfo file,
        CancellationToken ct)
    {
        var overallStopwatch = Stopwatch.StartNew();
        var collectionName = fileSet.Definition.Name;

        logger.LogInformation("Starting ingestion of file {FileKey} into collection {CollectionName}",
            file.Key, collectionName);

        var collection = await EnsureCollectionExistsAsync(collectionName, ct);
        await EnsureWildcardIndexExistsAsync(collection, ct);

        string? tempFilePath = null;
        IngestionProgressTracker? progressTracker = null;

        try
        {
            // Track S3 download time
            var downloadStopwatch = Stopwatch.StartNew();
            tempFilePath = await DownloadToTempFileAsync(blobs, file.Key, ct);
            downloadStopwatch.Stop();

            logger.LogInformation("Downloaded file {FileKey} to temp storage: {TempPath} in {DownloadDuration}ms",
                file.Key, tempFilePath, downloadStopwatch.ElapsedMilliseconds);

            // Count rows for progress tracking
            var estimatedRowCount = await _rowCounter.CountRowsAsync(tempFilePath, ct);
            progressTracker = new IngestionProgressTracker(file.Key, estimatedRowCount);

            logger.LogInformation("File {FileKey} has approximately {RowCount} data rows to process",
                file.Key, estimatedRowCount);

            // Track MongoDB ingestion time
            var mongoIngestionStopwatch = Stopwatch.StartNew();

            var csvContext = await OpenCsvFileFromDiskAsync(tempFilePath, ct);

            var headers = await ReadAndValidateHeadersAsync(
                csvContext.Csv,
                file.Key,
                fileSet.Definition.PrimaryKeyHeaderNames,
                fileSet.Definition.ChangeTypeHeaderName);

            var metrics = await ProcessCsvRecordsAsync(
                importId,
                collection,
                csvContext.Csv,
                headers,
                file.Key,
                collectionName,
                fileSet.Definition,
                progressTracker,
                ct);

            await csvContext.DisposeAsync();

            mongoIngestionStopwatch.Stop();
            overallStopwatch.Stop();

            logger.LogInformation("Completed ingestion of file {FileKey}. Total records: {TotalRecords}, Created: {Created}, Updated: {Updated}, Deleted: {Deleted}, S3 Download: {DownloadDuration}ms, MongoDB Ingestion: {MongoIngestionDuration}ms, Total Duration: {TotalDuration}ms, Avg Record Processing: {AvgMs:F2}ms/record",
                file.Key,
                metrics.RecordsProcessed,
                metrics.RecordsCreated,
                metrics.RecordsUpdated,
                metrics.RecordsDeleted,
                downloadStopwatch.ElapsedMilliseconds,
                mongoIngestionStopwatch.ElapsedMilliseconds,
                overallStopwatch.ElapsedMilliseconds,
                metrics.AverageMongoIngestionMs);

            return metrics with
            {
                S3DownloadDurationMs = downloadStopwatch.ElapsedMilliseconds,
                MongoIngestionDurationMs = mongoIngestionStopwatch.ElapsedMilliseconds
            };
        }
        finally
        {
            // Ensure temp file is always cleaned up
            if (tempFilePath != null && File.Exists(tempFilePath))
            {
                try
                {
                    File.Delete(tempFilePath);
                    logger.LogDebug("Deleted temp file: {TempPath}", tempFilePath);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to delete temp file: {TempPath}", tempFilePath);
                }
            }
        }
    }

    private async Task<string> DownloadToTempFileAsync(
        IBlobStorageService blobs,
        string fileKey,
        CancellationToken ct)
    {
        var tempFilePath = Path.Combine(Path.GetTempPath(), $"keeper_import_{Guid.NewGuid():N}.csv");

        logger.LogDebug("Downloading {FileKey} to {TempPath}", fileKey, tempFilePath);

        await using var sourceStream = await blobs.OpenReadAsync(fileKey, ct);
        await using var fileStream = new FileStream(
            tempFilePath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            bufferSize: 81920, // 80KB buffer
            useAsync: true);

        await sourceStream.CopyToAsync(fileStream, ct);
        await fileStream.FlushAsync(ct);

        return tempFilePath;
    }

    private async Task<CsvContext> OpenCsvFileFromDiskAsync(
        string filePath,
        CancellationToken ct)
    {
        var stream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 81920, // 80KB buffer
            useAsync: true);

        var reader = new StreamReader(stream);
        var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            TrimOptions = TrimOptions.Trim,
            BadDataFound = null, // Ignore bad data
            Delimiter = "|" // Use pipe delimiter for CSV files
        });

        return new CsvContext(stream, reader, csv);
    }

    private async Task<CsvHeaders> ReadAndValidateHeadersAsync(
        CsvReader csv,
        string fileKey,
        string[] primaryKeyHeaderNames,
        string changeTypeHeaderName)
    {
        await csv.ReadAsync();
        csv.ReadHeader();
        var rawHeaders = csv.HeaderRecord;

        if (rawHeaders == null || rawHeaders.Length == 0)
        {
            logger.LogWarning("No headers found in file {FileKey}, skipping ingestion", fileKey);
            throw new InvalidOperationException($"No headers found in file {fileKey}");
        }

        // Normalize headers by removing surrounding quotes
        var headers = NormalizeHeaders(rawHeaders);

        ValidatePrimaryKeyHeaders(headers, primaryKeyHeaderNames);
        ValidateChangeTypeHeader(headers, changeTypeHeaderName);

        logger.LogInformation("CSV headers read successfully. Total columns: {ColumnCount}, Primary Keys: {PrimaryKeys}, Change Type: {ChangeType}",
            headers.Length, string.Join(", ", primaryKeyHeaderNames), changeTypeHeaderName);

        return new CsvHeaders(headers, primaryKeyHeaderNames, changeTypeHeaderName);
    }

    private string[] NormalizeHeaders(string[] headers)
    {
        return headers.Select(h => h?.Trim('"') ?? string.Empty).ToArray();
    }

    private void ValidatePrimaryKeyHeaders(string[] headers, string[] primaryKeyHeaderNames)
    {
        var missingHeaders = primaryKeyHeaderNames.Where(pk => !headers.Contains(pk)).ToList();

        if (missingHeaders.Count > 0)
        {
            throw new InvalidOperationException(
        $"Primary key header(s) '{string.Join(", ", missingHeaders)}' not found in CSV headers. Available headers: {string.Join(", ", headers)}");
        }
    }

    private void ValidateChangeTypeHeader(string[] headers, string changeTypeHeaderName)
    {
        if (!headers.Contains(changeTypeHeaderName))
        {
            throw new InvalidOperationException(
     $"Change type header '{changeTypeHeaderName}' not found in CSV headers. Available headers: {string.Join(", ", headers)}");
        }
    }

    private const string CompositeKeyDelimiter = "__";

    private async Task<FileIngestionMetrics> ProcessCsvRecordsAsync(
        Guid importId,
        IMongoCollection<BsonDocument> collection,
        CsvReader csv,
        CsvHeaders headers,
        string fileKey,
        string collectionName,
        DataSetDefinition definition,
        IngestionProgressTracker progressTracker,
        CancellationToken ct)
    {
        var metrics = new RecordMetricsAccumulator();
        var batch = new List<(BsonDocument Document, string ChangeType)>();
        var lineageEvents = new List<RecordLineageEvent>();
        var totalMongoProcessingMs = 0L;
        var totals = new IngestionTotals();

        while (await csv.ReadAsync())
        {
            var changeType = csv.GetField(headers.ChangeTypeHeaderName)?.ToUpperInvariant() ?? string.Empty;

            if (!IsValidChangeType(changeType))
            {
                var primaryKeyValue = string.Join(CompositeKeyDelimiter, headers.PrimaryKeyHeaderNames.Select(pkHeader => csv.GetField(pkHeader) ?? string.Empty));
                logger.LogWarning("Invalid change type '{ChangeType}' for record with primary key '{PrimaryKey}' in file {FileKey}, skipping record",
                    changeType, primaryKeyValue, fileKey);
                metrics.RecordsSkipped++;
                continue;
            }

            var document = CreateDocumentFromCsvRecord(csv, headers, definition);
            batch.Add((document, changeType));

            if (batch.Count >= BatchSize)
            {
                var batchStopwatch = Stopwatch.StartNew();

                var batchMetrics = await ProcessBatchAsync(
                    importId,
                    collection,
                    batch,
                    fileKey,
                    collectionName,
                    definition,
                    lineageEvents,
                    ct);

                batchStopwatch.Stop();
                totalMongoProcessingMs += batchStopwatch.ElapsedMilliseconds;

                metrics.AddBatch(batchMetrics);
                totals = totals.Add(new IngestionTotals
                {
                    RecordsCreated = batchMetrics.RecordsCreated,
                    RecordsUpdated = batchMetrics.RecordsUpdated,
                    RecordsDeleted = batchMetrics.RecordsDeleted
                });

                // Update progress tracking and report every 100 records
                progressTracker.UpdateProgress(metrics.RecordsProcessed);

                if (metrics.RecordsProcessed % LogInterval == 0)
                {
                    LogProgressIfNeeded(metrics.RecordsProcessed, fileKey);

                    var currentStatus = progressTracker.GetCurrentStatus();
                    await UpdateIngestionPhaseProgressWithFileStatusAsync(
                        importId,
                        totals,
                        currentStatus,
                        ct);
                }

                batch.Clear();

                await FlushLineageEventsIfNeededAsync(lineageEvents, ct);
            }
        }

        // Process remaining records
        if (batch.Count > 0)
        {
            var batchMetrics = await ProcessBatchAsync(
                importId,
                collection,
                batch,
                fileKey,
                collectionName,
                definition,
                lineageEvents,
                ct);

            metrics.AddBatch(batchMetrics);

            // Final progress update
            progressTracker.UpdateProgress(metrics.RecordsProcessed);
            var finalStatus = progressTracker.Complete();

            await UpdateIngestionPhaseProgressWithFileStatusAsync(
                importId,
                totals,
                finalStatus,
                ct);
        }

        // Flush remaining lineage events
        await FlushLineageEventsAsync(lineageEvents, ct);

        // Calculate average MongoDB ingestion time per record
        var avgMongoMs = metrics.RecordsProcessed > 0
            ? (double)totalMongoProcessingMs / metrics.RecordsProcessed
     : 0;

        return metrics.ToFileMetrics(avgMongoMs);
    }

    private bool IsValidChangeType(string changeType)
    {
        return changeType == ChangeType.Delete ||
               changeType == ChangeType.Update ||
               changeType == ChangeType.Insert;
    }

    private void LogProgressIfNeeded(int recordsProcessed, string fileKey)
    {
        if (recordsProcessed % (LogInterval * BatchSize) == 0 || recordsProcessed % LogInterval == 0)
        {
            logger.LogInformation("Imported {RecordsProcessed} records from file {FileKey}",
                recordsProcessed, fileKey);
        }
    }

    private async Task FlushLineageEventsIfNeededAsync(
        List<RecordLineageEvent> lineageEvents,
        CancellationToken ct)
    {
        if (lineageEvents.Count >= LineageEventBatchSize)
        {
            await reportingService.RecordLineageEventsBatchAsync(lineageEvents, ct);
            lineageEvents.Clear();
        }
    }

    private async Task FlushLineageEventsAsync(
        List<RecordLineageEvent> lineageEvents,
        CancellationToken ct)
    {
        if (lineageEvents.Count > 0)
        {
            await reportingService.RecordLineageEventsBatchAsync(lineageEvents, ct);
        }
    }

    private async Task<IMongoCollection<BsonDocument>> EnsureCollectionExistsAsync(
        string collectionName,
        CancellationToken ct)
    {
        var database = mongoClient.GetDatabase(_databaseConfig.DatabaseName);

        var collections = await database.ListCollectionNamesAsync(cancellationToken: ct);
        var collectionList = await collections.ToListAsync(ct);

        if (!collectionList.Contains(collectionName))
        {
            logger.LogInformation("Creating collection {CollectionName}", collectionName);
            await database.CreateCollectionAsync(collectionName, cancellationToken: ct);
        }

        return database.GetCollection<BsonDocument>(collectionName);
    }

    private async Task EnsureWildcardIndexExistsAsync(
        IMongoCollection<BsonDocument> collection,
        CancellationToken ct)
    {
        try
        {
            if (await WildcardIndexExistsAsync(collection, ct))
            {
                logger.LogDebug("Wildcard index already exists on collection {CollectionName}",
                    collection.CollectionNamespace.CollectionName);
                return;
            }

            await CreateWildcardIndexAsync(collection, ct);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to create wildcard index on collection {CollectionName}. Continuing with ingestion.",
                collection.CollectionNamespace.CollectionName);
        }
    }

    private async Task<bool> WildcardIndexExistsAsync(
        IMongoCollection<BsonDocument> collection,
        CancellationToken ct)
    {
        var indexes = await collection.Indexes.ListAsync(ct);
        var indexList = await indexes.ToListAsync(ct);

        return indexList.Any(index =>
        {
            if (index.TryGetValue("key", out var key) && key.IsBsonDocument)
            {
                var keyDoc = key.AsBsonDocument;
                return keyDoc.Contains("$**");
            }
            return false;
        });
    }

    private async Task CreateWildcardIndexAsync(
        IMongoCollection<BsonDocument> collection,
        CancellationToken ct)
    {
        logger.LogInformation("Creating wildcard index on collection {CollectionName}",
            collection.CollectionNamespace.CollectionName);

        var wildcardIndexKeys = Builders<BsonDocument>.IndexKeys.Wildcard();
        var indexModel = new CreateIndexModel<BsonDocument>(wildcardIndexKeys);

        await collection.Indexes.CreateOneAsync(indexModel, cancellationToken: ct);

        logger.LogInformation("Wildcard index created successfully on collection {CollectionName}",
            collection.CollectionNamespace.CollectionName);
    }

    private BsonDocument CreateDocumentFromCsvRecord(
        CsvReader csv,
        CsvHeaders headers,
        DataSetDefinition definition)
    {
        var document = new BsonDocument();
        var now = DateTime.UtcNow;
        var accumulatorSet = new HashSet<string>(definition.Accumulators ?? []);

        // Create composite primary key for _id
        var compositeKeyParts = headers.PrimaryKeyHeaderNames.Select(pkHeader => csv.GetField(pkHeader) ?? string.Empty);
        document[FieldId] = string.Join(CompositeKeyDelimiter, compositeKeyParts);

        foreach (var header in headers.AllHeaders)
        {
            var value = csv.GetField(header);

            // Check if this field is an accumulator
            if (accumulatorSet.Contains(header))
            {
                // Initialize accumulator fields as arrays with single value (if not empty/null)
                if (!string.IsNullOrEmpty(value))
                {
                    document[header] = new BsonArray { value };
                }
                else
                {
                    document[header] = new BsonArray(); // Empty array for null/empty values
                }
            }
            else
            {
                // Add non-accumulator fields verbatim - treat empty strings as null
                document[header] = string.IsNullOrEmpty(value) ? BsonNull.Value : value;
            }
        }

        // Add audit fields
        document[FieldCreatedAtUtc] = now;
        document[FieldUpdatedAtUtc] = now;

        return document;
    }

    private async Task<BatchProcessingMetrics> ProcessBatchAsync(
        Guid importId,
        IMongoCollection<BsonDocument> collection,
        List<(BsonDocument Document, string ChangeType)> batch,
        string fileKey,
        string collectionName,
        DataSetDefinition definition,
        List<RecordLineageEvent> lineageEvents,
        CancellationToken ct)
    {
        if (batch.Count == 0)
        {
            return new BatchProcessingMetrics();
        }

        var documentIds = batch.Select(b => b.Document[FieldId]).ToList();
        var existingDocsDict = await FetchExistingDocumentsAsync(collection, documentIds, ct);
        var softDeletedIds = GetSoftDeletedIds(existingDocsDict);

        var bulkOps = new List<WriteModel<BsonDocument>>();
        var metrics = new BatchMetricsAccumulator();

        foreach (var (document, changeType) in batch)
        {
            var docId = document[FieldId];
            var recordId = docId.ToString() ?? string.Empty;
            var existingDoc = existingDocsDict.GetValueOrDefault(docId);

            if (changeType == ChangeType.Delete)
            {
                ProcessDeleteOperation(bulkOps,
                                       lineageEvents,
                                       docId,
                                       recordId,
                                       existingDoc,
                                       importId,
                                       fileKey,
                                       collectionName,
                                       changeType);

                metrics.Deleted++;
                metrics.Processed++;
            }
            else if (changeType == ChangeType.Insert || changeType == ChangeType.Update)
            {
                var isSoftDeleted = softDeletedIds.Contains(docId);
                var isCreate = existingDoc == null;

                ProcessUpsertOperation(bulkOps,
                                       lineageEvents,
                                       document,
                                       docId,
                                       recordId,
                                       existingDoc,
                                       isCreate,
                                       isSoftDeleted,
                                       importId,
                                       fileKey,
                                       collectionName,
                                       changeType,
                                       definition);

                if (isCreate)
                {
                    metrics.Created++;
                }
                else
                {
                    metrics.Updated++;
                }
                metrics.Processed++;
            }
        }

        if (bulkOps.Count > 0)
        {
            await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false }, ct);
        }

        return metrics.ToMetrics();
    }

    private async Task<Dictionary<BsonValue, BsonDocument>> FetchExistingDocumentsAsync(
        IMongoCollection<BsonDocument> collection,
     List<BsonValue> documentIds,
      CancellationToken ct)
    {
        var filter = Builders<BsonDocument>.Filter.In(FieldId, documentIds);
        var existingDocs = await collection.Find(filter).ToListAsync(ct);
        return existingDocs.ToDictionary(doc => doc[FieldId], doc => doc);
    }

    private HashSet<BsonValue> GetSoftDeletedIds(Dictionary<BsonValue, BsonDocument> existingDocsDict)
    {
        return existingDocsDict
   .Where(kvp => kvp.Value.Contains(FieldIsDeleted) && kvp.Value[FieldIsDeleted].AsBoolean)
     .Select(kvp => kvp.Key)
   .ToHashSet();
    }

    private void ProcessDeleteOperation(List<WriteModel<BsonDocument>> bulkOps,
                                        List<RecordLineageEvent> lineageEvents,
                                        BsonValue docId,
                                        string recordId,
                                        BsonDocument? existingDoc,
                                        Guid importId,
                                        string fileKey,
                                        string collectionName,
                                        string changeType)
    {
        var eventTime = DateTime.UtcNow;

        var deleteFilter = Builders<BsonDocument>.Filter.Eq(FieldId, docId);
        var deleteUpdate = Builders<BsonDocument>.Update
            .Set(FieldIsDeleted, true)
            .Set(FieldDeletedAtUtc, eventTime)
            .Set(FieldUpdatedAtUtc, eventTime);

        bulkOps.Add(new UpdateOneModel<BsonDocument>(deleteFilter, deleteUpdate));

        lineageEvents.Add(new RecordLineageEvent
        {
            RecordId = recordId,
            CollectionName = collectionName,
            EventType = RecordEventType.Deleted,
            ImportId = importId,
            FileKey = fileKey,
            EventDateUtc = eventTime,
            ChangeType = changeType,
            PreviousValues = existingDoc,
            NewValues = null
        });
    }

    private void ProcessUpsertOperation(List<WriteModel<BsonDocument>> bulkOps,
                                        List<RecordLineageEvent> lineageEvents,
                                        BsonDocument document,
                                        BsonValue docId,
                                        string recordId,
                                        BsonDocument? existingDoc,
                                        bool isCreate,
                                        bool isSoftDeleted,
                                        Guid importId,
                                        string fileKey,
                                        string collectionName,
                                        string changeType,
                                        DataSetDefinition definition)
    {
        var eventTime = DateTime.UtcNow;

        var upsertFilter = Builders<BsonDocument>.Filter.Eq(FieldId, docId);

        document[FieldIsDeleted] = false;

        var update = Builders<BsonDocument>.Update
            .SetOnInsert(FieldCreatedAtUtc, document[FieldCreatedAtUtc])
            .Set(FieldUpdatedAtUtc, document[FieldUpdatedAtUtc])
            .Set(FieldIsDeleted, false)
            .Unset(FieldDeletedAtUtc); // Unset DeletedAtUtc when undeleting

        var accumulatorSet = new HashSet<string>(definition.Accumulators ?? []);

        foreach (var element in document.Elements)
        {
            if (element.Name != FieldId && element.Name != FieldCreatedAtUtc && element.Name != FieldUpdatedAtUtc && element.Name != FieldIsDeleted)
            {
                if (accumulatorSet.Contains(element.Name))
                {
                    if (element.Value.IsBsonArray)
                    {
                        var arrayValue = element.Value.AsBsonArray;

                        if (arrayValue.Count == 0)
                        {
                            update = update.SetOnInsert(element.Name, new BsonArray());
                        }
                        else
                        {
                            foreach (var item in arrayValue)
                            {
                                // Only add non-null values
                                if (item != BsonNull.Value && !string.IsNullOrEmpty(item.ToString()))
                                {
                                    update = update.AddToSet(element.Name, item);
                                }
                            }
                        }
                    }
                    else if (element.Value != BsonNull.Value && !string.IsNullOrEmpty(element.Value.ToString()))
                    {
                        update = update.AddToSet(element.Name, element.Value);
                    }
                }
                else
                {
                    // For non-accumulator fields, use Set to overwrite
                    update = update.Set(element.Name, element.Value);
                }
            }
        }

        bulkOps.Add(new UpdateOneModel<BsonDocument>(upsertFilter, update)
        {
            IsUpsert = true
        });

        // Determine the correct event type based on the operation
        RecordEventType eventType;
        if (isSoftDeleted)
        {
            eventType = RecordEventType.Undeleted;
            logger.LogInformation("Undeleting soft-deleted record with _id: {DocId} in file {FileKey}", docId, fileKey);
        }
        else if (isCreate)
        {
            eventType = RecordEventType.Created;
        }
        else
        {
            eventType = RecordEventType.Updated;
        }

        lineageEvents.Add(new RecordLineageEvent
        {
            RecordId = recordId,
            CollectionName = collectionName,
            EventType = eventType,
            ImportId = importId,
            FileKey = fileKey,
            EventDateUtc = eventTime,
            ChangeType = changeType,
            PreviousValues = existingDoc,
            NewValues = document
        });
    }

    private async Task RecordSuccessfulIngestionAsync(
        Guid importId,
        StorageObjectInfo file,
        FileIngestionMetrics metrics,
        long durationMs,
        CancellationToken ct)
    {
        await reportingService.RecordFileIngestionAsync(importId, new FileIngestionRecord
        {
            FileKey = file.Key,
            RecordsProcessed = metrics.RecordsProcessed,
            RecordsCreated = metrics.RecordsCreated,
            RecordsUpdated = metrics.RecordsUpdated,
            RecordsDeleted = metrics.RecordsDeleted,
            IngestionDurationMs = durationMs,
            AverageRecordIngestionMs = metrics.AverageMongoIngestionMs,
            S3DownloadDurationMs = metrics.S3DownloadDurationMs,
            MongoIngestionDurationMs = metrics.MongoIngestionDurationMs,
            IngestedAtUtc = DateTime.UtcNow,
            Status = FileProcessingStatus.Ingested
        }, ct);
    }

    private async Task RecordFailedIngestionAsync(
        Guid importId,
        StorageObjectInfo file,
     long durationMs,
        Exception ex,
     CancellationToken ct)
    {
        try
        {
            await reportingService.RecordFileIngestionAsync(importId, new FileIngestionRecord
            {
                FileKey = file.Key,
                RecordsProcessed = 0,
                RecordsCreated = 0,
                RecordsUpdated = 0,
                RecordsDeleted = 0,
                IngestionDurationMs = durationMs,
                AverageRecordIngestionMs = 0,
                S3DownloadDurationMs = 0,
                MongoIngestionDurationMs = 0,
                IngestedAtUtc = DateTime.UtcNow,
                Status = FileProcessingStatus.Failed,
                Error = ex.Message
            }, ct);
        }
        catch (Exception reportEx)
        {
            logger.LogError(reportEx, "Failed to record ingestion failure for file: {FileKey}", file.Key);
        }
    }

    private async Task UpdateIngestionPhaseProgressAsync(
        Guid importId,
        int filesProcessed,
        IngestionTotals totals,
        IngestionCurrentFileStatus? currentFileStatus,
        CancellationToken ct)
    {
        await reportingService.UpdateIngestionPhaseAsync(importId, new IngestionPhaseUpdate
        {
            Status = PhaseStatus.Started,
            FilesProcessed = filesProcessed,
            RecordsCreated = totals.RecordsCreated,
            RecordsUpdated = totals.RecordsUpdated,
            RecordsDeleted = totals.RecordsDeleted,
            CurrentFileStatus = currentFileStatus
        }, ct);
    }

    private async Task UpdateIngestionPhaseProgressWithFileStatusAsync(
        Guid importId,
        IngestionTotals totals,
        IngestionCurrentFileStatus currentFileStatus,
        CancellationToken ct)
    {
        await reportingService.UpdateIngestionPhaseAsync(importId, new IngestionPhaseUpdate
        {
            Status = PhaseStatus.Started,
            FilesProcessed = 0, // Not updated during file processing
            RecordsCreated = totals.RecordsCreated,
            RecordsUpdated = totals.RecordsUpdated,
            RecordsDeleted = totals.RecordsDeleted,
            CurrentFileStatus = currentFileStatus
        }, ct);
    }

    private async Task UpdateIngestionPhaseCompletedAsync(
        Guid importId,
        int filesProcessed,
        int recordsCreated,
        int recordsUpdated,
        int recordsDeleted,
        CancellationToken ct)
    {
        await reportingService.UpdateIngestionPhaseAsync(importId, new IngestionPhaseUpdate
        {
            Status = PhaseStatus.Completed,
            FilesProcessed = filesProcessed,
            RecordsCreated = recordsCreated,
            RecordsUpdated = recordsUpdated,
            RecordsDeleted = recordsDeleted,
            CompletedAtUtc = DateTime.UtcNow
        }, ct);
    }

    private void LogPipelineCompletion(Guid importId, Stopwatch stopwatch)
    {
        stopwatch.Stop();
        logger.LogInformation("Import pipeline completed successfully for ImportId: {ImportId}. Total duration: {Duration}ms ({DurationSeconds}s)",
            importId,
            stopwatch.ElapsedMilliseconds,
            stopwatch.Elapsed.TotalSeconds);
    }

    private void LogPipelineFailure(Guid importId, Stopwatch stopwatch, Exception ex)
    {
        stopwatch.Stop();
        logger.LogError(ex, "Import pipeline failed for ImportId: {ImportId} after {Duration}ms ({DurationSeconds}s)",
            importId,
            stopwatch.ElapsedMilliseconds,
            stopwatch.Elapsed.TotalSeconds);
    }

    // Helper records and classes for internal state management
    private record CsvContext(Stream Stream, StreamReader Reader, CsvReader Csv) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            Csv?.Dispose();
            Reader?.Dispose();
            await (Stream?.DisposeAsync() ?? ValueTask.CompletedTask);
        }
    }

    private record CsvHeaders(string[] AllHeaders, string[] PrimaryKeyHeaderNames, string ChangeTypeHeaderName);

    private record IngestionTotals
    {
        public int FilesProcessed { get; init; }
        public int RecordsCreated { get; init; }
        public int RecordsUpdated { get; init; }
        public int RecordsDeleted { get; init; }

        public IngestionTotals Add(IngestionTotals other) => new()
        {
            FilesProcessed = FilesProcessed + other.FilesProcessed,
            RecordsCreated = RecordsCreated + other.RecordsCreated,
            RecordsUpdated = RecordsUpdated + other.RecordsUpdated,
            RecordsDeleted = RecordsDeleted + other.RecordsDeleted
        };
    }

    private class RecordMetricsAccumulator
    {
        public int RecordsProcessed { get; set; }
        public int RecordsSkipped { get; set; }
        public int RecordsCreated { get; set; }
        public int RecordsUpdated { get; set; }
        public int RecordsDeleted { get; set; }

        public void AddBatch(BatchProcessingMetrics batchMetrics)
        {
            RecordsProcessed += batchMetrics.RecordsProcessed;
            RecordsCreated += batchMetrics.RecordsCreated;
            RecordsUpdated += batchMetrics.RecordsUpdated;
            RecordsDeleted += batchMetrics.RecordsDeleted;
        }

        public FileIngestionMetrics ToFileMetrics(double avgMongoIngestionMs) => new()
        {
            RecordsProcessed = RecordsProcessed,
            RecordsCreated = RecordsCreated,
            RecordsUpdated = RecordsUpdated,
            RecordsDeleted = RecordsDeleted,
            AverageMongoIngestionMs = avgMongoIngestionMs
        };
    }

    private class BatchMetricsAccumulator
    {
        public int Processed { get; set; }
        public int Created { get; set; }
        public int Updated { get; set; }
        public int Deleted { get; set; }

        public BatchProcessingMetrics ToMetrics() => new()
        {
            RecordsProcessed = Processed,
            RecordsCreated = Created,
            RecordsUpdated = Updated,
            RecordsDeleted = Deleted
        };
    }

    private record FileIngestionMetrics
    {
        public int RecordsProcessed { get; init; }
        public int RecordsCreated { get; init; }
        public int RecordsUpdated { get; init; }
        public int RecordsDeleted { get; init; }
        public double AverageMongoIngestionMs { get; init; }
        public long S3DownloadDurationMs { get; init; }
        public long MongoIngestionDurationMs { get; init; }
    }

    private record BatchProcessingMetrics
    {
        public int RecordsProcessed { get; init; }
        public int RecordsCreated { get; init; }
        public int RecordsUpdated { get; init; }
        public int RecordsDeleted { get; init; }
    }
}