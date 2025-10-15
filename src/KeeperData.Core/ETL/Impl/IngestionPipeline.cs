using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using MongoDB.Driver;
using MongoDB.Bson;
using CsvHelper;
using System.Globalization;
using CsvHelper.Configuration;
using Microsoft.Extensions.Options;
using KeeperData.Core.Database;

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
    ILogger<IngestionPipeline> logger) : IIngestionPipeline
{
    private const int BatchSize = 1000;
    private const int LogInterval = 100;
    private const int LineageBatchSize = 100;
    private readonly IDatabaseConfig _databaseConfig = databaseConfig.Value;

    public async Task StartAsync(Guid importId, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        logger.LogInformation("Starting ingest pipeline for ImportId: {ImportId}", importId);

        try
        {
            var blobs = blobStorageServiceFactory.Get();
            var ExternalCatalogueService = ExternalCatalogueServiceFactory.Create(blobs);

            logger.LogDebug("Initialized blob storage services for ImportId: {ImportId}", importId);

            // step 1: discover files that may need processing
            logger.LogInformation("Step 1: Discovering files for ImportId: {ImportId}", importId);
            var fileSets = await ExternalCatalogueService.GetFileSetsAsync(20, ct);
            var totalFiles = fileSets.Sum(fs => fs.Files.Length);
            
            logger.LogInformation("Discovered {FileSetCount} file set(s) containing {TotalFileCount} file(s) for ImportId: {ImportId}",
                fileSets.Count,
                totalFiles,
                importId);

            // Update ingestion phase - started
            await reportingService.UpdateIngestionPhaseAsync(importId, new IngestionPhaseUpdate
            {
                Status = PhaseStatus.Started,
                FilesProcessed = 0,
                RecordsCreated = 0,
                RecordsUpdated = 0,
                RecordsDeleted = 0
            }, ct);

            // step 2: for each file, ingest into mongo
            logger.LogInformation("Step 2: Ingesting files for ImportId: {ImportId}", importId);
            var processedFileCount = 0;
            var failedFileCount = 0;
            var totalRecordsCreated = 0;
            var totalRecordsUpdated = 0;
            var totalRecordsDeleted = 0;

            foreach (var fileSet in fileSets)
            {
                logger.LogDebug("Processing file set for definition: {DefinitionName} with {FileCount} file(s) for ImportId: {ImportId}",
                    fileSet.Definition.Name,
                    fileSet.Files.Length,
                    importId);

                foreach (var file in fileSet.Files)
                {
                    var fileStopwatch = Stopwatch.StartNew();
                    processedFileCount++;

                    logger.LogInformation("Processing file {CurrentFile}/{TotalFiles}: {FileKey} for ImportId: {ImportId}",
                        processedFileCount,
                        totalFiles,
                        file.Key,
                        importId);

                    try
                    {
                        var fileStats = await IngestAsync(importId, blobs, fileSet, file, ct);
                        
                        fileStopwatch.Stop();

                        // Record successful ingestion
                        await reportingService.RecordFileIngestionAsync(importId, new FileIngestionRecord
                        {
                            FileKey = file.Key,
                            RecordsProcessed = fileStats.RecordsProcessed,
                            RecordsCreated = fileStats.RecordsCreated,
                            RecordsUpdated = fileStats.RecordsUpdated,
                            RecordsDeleted = fileStats.RecordsDeleted,
                            IngestionDurationMs = fileStopwatch.ElapsedMilliseconds,
                            IngestedAtUtc = DateTime.UtcNow,
                            Status = FileProcessingStatus.Ingested
                        }, ct);

                        totalRecordsCreated += fileStats.RecordsCreated;
                        totalRecordsUpdated += fileStats.RecordsUpdated;
                        totalRecordsDeleted += fileStats.RecordsDeleted;

                        logger.LogInformation("Successfully processed file: {FileKey} in {Duration}ms for ImportId: {ImportId}",
                            file.Key,
                            fileStopwatch.ElapsedMilliseconds,
                            importId);
                    }
                    catch (Exception ex)
                    {
                        fileStopwatch.Stop();
                        failedFileCount++;
                        
                        logger.LogError(ex, "Failed to process file: {FileKey} after {Duration}ms for ImportId: {ImportId}",
                            file.Key,
                            fileStopwatch.ElapsedMilliseconds,
                            importId);

                        // Record failed ingestion
                        try
                        {
                            await reportingService.RecordFileIngestionAsync(importId, new FileIngestionRecord
                            {
                                FileKey = file.Key,
                                RecordsProcessed = 0,
                                RecordsCreated = 0,
                                RecordsUpdated = 0,
                                RecordsDeleted = 0,
                                IngestionDurationMs = fileStopwatch.ElapsedMilliseconds,
                                IngestedAtUtc = DateTime.UtcNow,
                                Status = FileProcessingStatus.Failed,
                                Error = ex.Message
                            }, ct);
                        }
                        catch (Exception reportEx)
                        {
                            logger.LogError(reportEx, "Failed to record ingestion failure for file: {FileKey}", file.Key);
                        }
                        
                        throw;
                    }
                }
            }

            logger.LogInformation("Step 2 completed: Processed {ProcessedFileCount} file(s) for ImportId: {ImportId}",
                processedFileCount,
                importId);

            // Update ingestion phase - completed
            await reportingService.UpdateIngestionPhaseAsync(importId, new IngestionPhaseUpdate
            {
                Status = failedFileCount > 0 ? PhaseStatus.Failed : PhaseStatus.Completed,
                FilesProcessed = processedFileCount - failedFileCount,
                RecordsCreated = totalRecordsCreated,
                RecordsUpdated = totalRecordsUpdated,
                RecordsDeleted = totalRecordsDeleted,
                CompletedAtUtc = DateTime.UtcNow
            }, ct);

            stopwatch.Stop();
            logger.LogInformation("Import pipeline completed successfully for ImportId: {ImportId}. Total duration: {Duration}ms ({DurationSeconds}s)",
                importId,
                stopwatch.ElapsedMilliseconds,
                stopwatch.Elapsed.TotalSeconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            logger.LogError(ex, "Import pipeline failed for ImportId: {ImportId} after {Duration}ms ({DurationSeconds}s)",
                importId,
                stopwatch.ElapsedMilliseconds,
                stopwatch.Elapsed.TotalSeconds);
            throw;
        }
    }

    private async Task<FileIngestionStats> IngestAsync(
        Guid importId, 
        IBlobStorageService blobs, 
        FileSet fileSet, 
        StorageObjectInfo file, 
        CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        var collectionName = fileSet.Definition.Name;
        var primaryKeyHeaderName = fileSet.Definition.PrimaryKeyHeaderName;
        var changeTypeHeaderName = fileSet.Definition.ChangeTypeHeaderName;

        logger.LogInformation("Starting ingestion of file {FileKey} into collection {CollectionName}", file.Key, collectionName);

        // Ensure collection exists and has wildcard index
        var collection = await EnsureCollectionExistsAsync(collectionName, ct);
        await EnsureWildcardIndexExistsAsync(collection, ct);

        // Open stream to CSV file
        await using var stream = await blobs.OpenReadAsync(file.Key, ct);
        using var reader = new StreamReader(stream);
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            TrimOptions = TrimOptions.Trim,
            BadDataFound = null // Ignore bad data
        });

        // Read header
        await csv.ReadAsync();
        csv.ReadHeader();
        var headers = csv.HeaderRecord;

        if (headers == null || headers.Length == 0)
        {
            logger.LogWarning("No headers found in file {FileKey}, skipping ingestion", file.Key);
            return new FileIngestionStats();
        }

        // Validate primary key header exists
        if (!headers.Contains(primaryKeyHeaderName))
        {
            throw new InvalidOperationException(
                $"Primary key header '{primaryKeyHeaderName}' not found in CSV headers. Available headers: {string.Join(", ", headers)}");
        }

        // Validate change type header exists
        if (!headers.Contains(changeTypeHeaderName))
        {
            throw new InvalidOperationException(
                $"Change type header '{changeTypeHeaderName}' not found in CSV headers. Available headers: {string.Join(", ", headers)}");
        }

        logger.LogInformation("CSV headers read successfully. Total columns: {ColumnCount}, Primary Key: {PrimaryKey}, Change Type: {ChangeType}", 
            headers.Length, primaryKeyHeaderName, changeTypeHeaderName);

        // Process records in batches
        var stats = new FileIngestionStats();
        var batch = new List<(BsonDocument Document, string ChangeType)>();

        while (await csv.ReadAsync())
        {
            var changeType = csv.GetField(changeTypeHeaderName)?.ToUpperInvariant() ?? string.Empty;
            
            // Validate change type
            if (changeType != ChangeType.Delete && 
                changeType != ChangeType.Update && 
                changeType != ChangeType.Insert)
            {
                logger.LogWarning("Invalid change type '{ChangeType}' for record with primary key '{PrimaryKey}' in file {FileKey}, skipping record",
                    changeType, csv.GetField(primaryKeyHeaderName), file.Key);
                continue;
            }

            var document = CreateDocumentFromCsvRecord(csv, headers, primaryKeyHeaderName);
            batch.Add((document, changeType));

            if (batch.Count >= BatchSize)
            {
                var batchStats = await ProcessBatchAsync(importId, collectionName, collection, batch, file.Key, ct);
                stats.Add(batchStats);
                
                if (stats.RecordsProcessed % (LogInterval * BatchSize) == 0 || stats.RecordsProcessed % LogInterval == 0)
                {
                    logger.LogInformation("Imported {RecordsProcessed} records from file {FileKey}", stats.RecordsProcessed, file.Key);
                }
                
                batch.Clear();
            }
        }

        // Process remaining records
        if (batch.Count > 0)
        {
            var batchStats = await ProcessBatchAsync(importId, collectionName, collection, batch, file.Key, ct);
            stats.Add(batchStats);
        }

        stopwatch.Stop();
        logger.LogInformation("Completed ingestion of file {FileKey}. Created: {Created}, Updated: {Updated}, Deleted: {Deleted}, Duration: {Duration}ms",
            file.Key, stats.RecordsCreated, stats.RecordsUpdated, stats.RecordsDeleted, stopwatch.ElapsedMilliseconds);

        return stats;
    }

    private async Task<IMongoCollection<BsonDocument>> EnsureCollectionExistsAsync(string collectionName, CancellationToken ct)
    {
        var database = mongoClient.GetDatabase(_databaseConfig.DatabaseName);

        // Check if collection exists
        var collections = await database.ListCollectionNamesAsync(cancellationToken: ct);
        var collectionList = await collections.ToListAsync(ct);

        if (!collectionList.Contains(collectionName))
        {
            logger.LogInformation("Creating collection {CollectionName}", collectionName);
            await database.CreateCollectionAsync(collectionName, cancellationToken: ct);
        }

        return database.GetCollection<BsonDocument>(collectionName);
    }

    private async Task EnsureWildcardIndexExistsAsync(IMongoCollection<BsonDocument> collection, CancellationToken ct)
    {
        try
        {
            // Check if wildcard index already exists
            var indexes = await collection.Indexes.ListAsync(ct);
            var indexList = await indexes.ToListAsync(ct);

            var hasWildcardIndex = indexList.Any(index =>
            {
                if (index.TryGetValue("key", out var key) && key.IsBsonDocument)
                {
                    var keyDoc = key.AsBsonDocument;
                    return keyDoc.Contains("$**");
                }
                return false;
            });

            if (!hasWildcardIndex)
            {
                logger.LogInformation("Creating wildcard index on collection {CollectionName}", collection.CollectionNamespace.CollectionName);

                var wildcardIndexKeys = Builders<BsonDocument>.IndexKeys.Wildcard("$**");
                var indexModel = new CreateIndexModel<BsonDocument>(wildcardIndexKeys);

                await collection.Indexes.CreateOneAsync(indexModel, cancellationToken: ct);

                logger.LogInformation("Wildcard index created successfully on collection {CollectionName}",
                    collection.CollectionNamespace.CollectionName);
            }
            else
            {
                logger.LogDebug("Wildcard index already exists on collection {CollectionName}",
                    collection.CollectionNamespace.CollectionName);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to create wildcard index on collection {CollectionName}. Continuing with ingestion.",
                collection.CollectionNamespace.CollectionName);
        }
    }

    private BsonDocument CreateDocumentFromCsvRecord(CsvReader csv, string[] headers, string primaryKeyHeaderName)
    {
        var document = new BsonDocument();
        var now = DateTime.UtcNow;

        foreach (var header in headers)
        {
            var value = csv.GetField(header);

            // Convert primary key to _id field
            if (header == primaryKeyHeaderName)
            {
                document["_id"] = value ?? string.Empty;
            }

            // Add all fields verbatim - treat empty strings as null
            if (string.IsNullOrEmpty(value))
            {
                document[header] = BsonNull.Value;
            }
            else
            {
                document[header] = value;
            }
        }

        // Add audit fields
        document["CreatedAtUtc"] = now;
        document["UpdatedAtUtc"] = now;

        return document;
    }

    private async Task<FileIngestionStats> ProcessBatchAsync(
        Guid importId,
        string collectionName,
        IMongoCollection<BsonDocument> collection,
        List<(BsonDocument Document, string ChangeType)> batch,
        string fileKey,
        CancellationToken ct)
    {
        if (batch.Count == 0)
        {
            return new FileIngestionStats();
        }

        var stats = new FileIngestionStats();
        var lineageEvents = new List<RecordLineageEvent>();
        var eventDateTime = DateTime.UtcNow;

        // Get all document IDs from the batch
        var documentIds = batch.Select(b => b.Document["_id"]).ToList();

        // Fetch existing documents to determine if they're new or updates, and to capture previous values
        var filter = Builders<BsonDocument>.Filter.In("_id", documentIds);
        var existingDocs = await collection.Find(filter).ToListAsync(ct);
        
        var existingDocsDict = existingDocs.ToDictionary(doc => doc["_id"].ToString() ?? string.Empty);
        var softDeletedIds = existingDocs
            .Where(doc => doc.Contains("IsDeleted") && doc["IsDeleted"].AsBoolean)
            .Select(doc => doc["_id"])
            .ToHashSet();

        var bulkOps = new List<WriteModel<BsonDocument>>();

        foreach (var (document, changeType) in batch)
        {
            var docId = document["_id"];
            var recordId = docId.ToString() ?? string.Empty;
            var isExisting = existingDocsDict.ContainsKey(recordId);

            if (changeType == ChangeType.Delete)
            {
                // Soft delete: set IsDeleted = true and DeletedAtUtc
                var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", docId);
                var deleteUpdate = Builders<BsonDocument>.Update
                    .Set("IsDeleted", true)
                    .Set("DeletedAtUtc", DateTime.UtcNow)
                    .Set("UpdatedAtUtc", DateTime.UtcNow);

                bulkOps.Add(new UpdateOneModel<BsonDocument>(deleteFilter, deleteUpdate));
                stats.RecordsDeleted++;
                stats.RecordsProcessed++;

                // Record lineage event for deletion
                if (isExisting)
                {
                    lineageEvents.Add(new RecordLineageEvent
                    {
                        RecordId = recordId,
                        CollectionName = collectionName,
                        EventType = RecordEventType.Deleted,
                        ImportId = importId,
                        FileKey = fileKey,
                        EventDateUtc = eventDateTime,
                        ChangeType = changeType,
                        PreviousValues = existingDocsDict[recordId],
                        NewValues = null
                    });
                }
            }
            else if (changeType == ChangeType.Insert || changeType == ChangeType.Update)
            {
                // Check if document is soft-deleted
                if (softDeletedIds.Contains(docId))
                {
                    // Skip insert/update for soft-deleted records
                    logger.LogDebug("Skipping {ChangeType} operation for soft-deleted record with _id: {DocId}", 
                        changeType, docId);
                    continue;
                }

                // Upsert the document
                var upsertFilter = Builders<BsonDocument>.Filter.Eq("_id", docId);
                
                // Ensure IsDeleted is false for active records
                document["IsDeleted"] = false;

                var update = Builders<BsonDocument>.Update
                    .SetOnInsert("CreatedAtUtc", document["CreatedAtUtc"])
                    .Set("UpdatedAtUtc", document["UpdatedAtUtc"]);

                // Set all other fields
                foreach (var element in document.Elements)
                {
                    if (element.Name != "_id" && element.Name != "CreatedAtUtc")
                    {
                        update = update.Set(element.Name, element.Value);
                    }
                }

                bulkOps.Add(new UpdateOneModel<BsonDocument>(upsertFilter, update)
                {
                    IsUpsert = true
                });

                // Track statistics
                if (isExisting)
                {
                    stats.RecordsUpdated++;
                }
                else
                {
                    stats.RecordsCreated++;
                }
                stats.RecordsProcessed++;

                // Record lineage event
                lineageEvents.Add(new RecordLineageEvent
                {
                    RecordId = recordId,
                    CollectionName = collectionName,
                    EventType = isExisting ? RecordEventType.Updated : RecordEventType.Created,
                    ImportId = importId,
                    FileKey = fileKey,
                    EventDateUtc = eventDateTime,
                    ChangeType = changeType,
                    PreviousValues = isExisting ? existingDocsDict[recordId] : null,
                    NewValues = document
                });
            }
        }

        // Execute bulk operations
        if (bulkOps.Count > 0)
        {
            await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false }, ct);
        }

        // Record lineage events in batches to avoid overwhelming the system
        if (lineageEvents.Count > 0)
        {
            try
            {
                // Process lineage events in smaller batches
                for (int i = 0; i < lineageEvents.Count; i += LineageBatchSize)
                {
                    var lineageBatch = lineageEvents.Skip(i).Take(LineageBatchSize);
                    await reportingService.RecordLineageEventsBatchAsync(lineageBatch, ct);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to record lineage events for batch. Continuing with ingestion.");
            }
        }

        return stats;
    }
}

internal class FileIngestionStats
{
    public int RecordsProcessed { get; set; }
    public int RecordsCreated { get; set; }
    public int RecordsUpdated { get; set; }
    public int RecordsDeleted { get; set; }

    public void Add(FileIngestionStats other)
    {
        RecordsProcessed += other.RecordsProcessed;
        RecordsCreated += other.RecordsCreated;
        RecordsUpdated += other.RecordsUpdated;
        RecordsDeleted += other.RecordsDeleted;
    }
}