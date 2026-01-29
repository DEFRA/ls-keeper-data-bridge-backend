using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using KeeperData.Core.Database;
using KeeperData.Core.Database.Resilience;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Storage;
using KeeperData.Core.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Core.ETL.Impl;

/// <summary>
/// This class is responsible for ingesting csv files into mongo.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Ingestion pipeline with MongoDB and S3 dependencies - covered by integration tests.")]
public class IngestionPipeline(
    IBlobStorageServiceFactory blobStorageServiceFactory,
    IExternalCatalogueServiceFactory ExternalCatalogueServiceFactory,
    IMongoClient mongoClient,
    IOptions<IDatabaseConfig> databaseConfig,
    IImportReportingService reportingService,
    CsvRowCounter csvRowCounter,
    ResilientMongoOperations resilientMongoOps,
    IApplicationMetrics metrics,
    ILogger<IngestionPipeline> logger) : IIngestionPipeline
{
    private const int BatchSize = 200;
    private const int BatchDelayMs = 500;
    private const int LogInterval = BatchSize;
    private const int ProgressUpdateInterval = BatchSize;

    private readonly IDatabaseConfig _databaseConfig = databaseConfig.Value;
    private readonly CsvRowCounter _rowCounter = csvRowCounter;
    private readonly RecordIdGenerator _recordIdGenerator = new();
    private readonly ResilientMongoOperations _resilientMongoOps = resilientMongoOps;

    // MongoDB field name constants
    private const string FieldId = "_id";
    private const string FieldCreatedAtUtc = "CreatedAtUtc";
    private const string FieldUpdatedAtUtc = "UpdatedAtUtc";
    private const string FieldIsDeleted = "IsDeleted";
    private const string FieldDeletedAtUtc = "DeletedAtUtc";

    public async Task StartAsync(ImportReport report, CancellationToken ct)
    {
        var ingestionStopwatch = Stopwatch.StartNew();
        var importId = report.ImportId;

        Debug.WriteLine($"[keepetl] Starting ingest pipeline for ImportId: {importId}");
        logger.LogInformation("Starting ingest pipeline for ImportId: {ImportId}", importId);

        metrics.RecordRequest(MetricNames.Ingestion, MetricNames.Operations.IngestionStarted);
        metrics.RecordCount(MetricNames.Ingestion, 1,
            (MetricNames.CommonTags.Operation, MetricNames.Operations.IngestionStarted),
            (MetricNames.CommonTags.SourceType, report.SourceType));

        try
        {
            var (blobStorage, catalogueService) = InitializeStorageServices(importId);

            var (fileSets, totalFiles) = await DiscoverFilesAsync(importId, catalogueService, ct);

            // Update ingestion phase to Started
            report.IngestionPhase!.Status = PhaseStatus.Started;
            report.IngestionPhase!.StartedAtUtc = DateTime.UtcNow;
            await reportingService.UpsertImportReportAsync(report, ct);

            var ingestionResults = await ProcessAllFilesAsync(report, fileSets, totalFiles, blobStorage, ct);

            // Update ingestion phase to Completed
            report.IngestionPhase!.Status = PhaseStatus.Completed;
            report.IngestionPhase!.FilesProcessed = ingestionResults.FilesProcessed;
            report.IngestionPhase!.RecordsCreated = ingestionResults.RecordsCreated;
            report.IngestionPhase!.RecordsUpdated = ingestionResults.RecordsUpdated;
            report.IngestionPhase!.RecordsDeleted = ingestionResults.RecordsDeleted;
            report.IngestionPhase!.CompletedAtUtc = DateTime.UtcNow;
            report.IngestionPhase!.CurrentFileStatus = null;
            await reportingService.UpsertImportReportAsync(report, ct);

            LogPipelineCompletion(importId, ingestionStopwatch);

            ingestionStopwatch.Stop();

            metrics.RecordRequest(MetricNames.Ingestion, MetricNames.Operations.IngestionCompletions);
            metrics.RecordValue(MetricNames.Ingestion, ingestionStopwatch.ElapsedMilliseconds,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.IngestionDuration),
                (MetricNames.CommonTags.SourceType, report.SourceType));
            metrics.RecordCount(MetricNames.Ingestion, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.IngestionCompletions),
                (MetricNames.CommonTags.SourceType, report.SourceType),
                (MetricNames.CommonTags.Status, "success"));

            var totalRecords = ingestionResults.RecordsCreated + ingestionResults.RecordsUpdated + ingestionResults.RecordsDeleted;
            if (totalRecords > 0 && ingestionStopwatch.ElapsedMilliseconds > 0)
            {
                var recordsPerMinute = (totalRecords * 60000.0) / ingestionStopwatch.ElapsedMilliseconds;
                metrics.RecordValue(MetricNames.Ingestion, recordsPerMinute,
                    (MetricNames.CommonTags.Operation, MetricNames.Operations.IngestionRecordsPerMinute),
                    (MetricNames.CommonTags.SourceType, report.SourceType));
            }

            metrics.RecordCount(MetricNames.Ingestion, ingestionResults.FilesProcessed,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.IngestionFilesProcessed),
                (MetricNames.CommonTags.SourceType, report.SourceType));
            metrics.RecordCount(MetricNames.Ingestion, ingestionResults.RecordsCreated,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.IngestionRecordsCreated),
                (MetricNames.CommonTags.SourceType, report.SourceType));
            metrics.RecordCount(MetricNames.Ingestion, ingestionResults.RecordsUpdated,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.IngestionRecordsUpdated),
                (MetricNames.CommonTags.SourceType, report.SourceType));
            metrics.RecordCount(MetricNames.Ingestion, ingestionResults.RecordsDeleted,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.IngestionRecordsDeleted),
                (MetricNames.CommonTags.SourceType, report.SourceType));
        }
        catch (Exception ex)
        {
            // Update ingestion phase to Failed
            report.IngestionPhase!.Status = PhaseStatus.Failed;
            report.IngestionPhase!.CompletedAtUtc = DateTime.UtcNow;
            await reportingService.UpsertImportReportAsync(report, ct);

            LogPipelineFailure(importId, ingestionStopwatch, ex);

            ingestionStopwatch.Stop();

            metrics.RecordRequest(MetricNames.Ingestion, MetricNames.Operations.IngestionErrors);
            metrics.RecordValue(MetricNames.Ingestion, ingestionStopwatch.ElapsedMilliseconds,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.IngestionDuration),
                (MetricNames.CommonTags.SourceType, report.SourceType));
            metrics.RecordCount(MetricNames.Ingestion, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.IngestionCompletions),
                (MetricNames.CommonTags.SourceType, report.SourceType),
                (MetricNames.CommonTags.Status, "failed"));
            metrics.RecordCount(MetricNames.Ingestion, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.IngestionErrors),
                (MetricNames.CommonTags.SourceType, report.SourceType),
                (MetricNames.CommonTags.ErrorType, ex.GetType().Name));

            throw;
        }
    }

    private (IBlobStorageService BlobStorage, ExternalCatalogueService CatalogueService) InitializeStorageServices(Guid importId)
    {
        Debug.WriteLine($"[keepetl] Initializing blob storage services for ImportId: {importId}");
        var blobs = blobStorageServiceFactory.Get();
        var catalogueService = ExternalCatalogueServiceFactory.Create(blobs);

        logger.LogDebug("Initialized blob storage services for ImportId: {ImportId}", importId);

        return (blobs, catalogueService);
    }

    private async Task<(ImmutableList<FileSet> FileSets, int TotalFiles)> DiscoverFilesAsync(Guid importId, ExternalCatalogueService catalogueService, CancellationToken ct)
    {
        Debug.WriteLine($"[keepetl] Step 1: Discovering files for ImportId: {importId}");
        logger.LogInformation("Step 1: Discovering files for ImportId: {ImportId}", importId);

        var fileSets = await catalogueService.GetFileSetsAsync(EtlConstants.DefaultLookbackDays, ct);
        var totalFiles = fileSets.Sum(fs => fs.Files.Length);

        Debug.WriteLine($"[keepetl] Discovered {fileSets.Count} file set(s) containing {totalFiles} file(s) for ImportId: {importId}");
        logger.LogInformation("Discovered {FileSetCount} file set(s) containing {TotalFileCount} file(s) for ImportId: {importId}", fileSets.Count, totalFiles, importId);

        return (fileSets, totalFiles);
    }

    private async Task<IngestionTotals> ProcessAllFilesAsync(ImportReport report, ImmutableList<FileSet> fileSets, int totalFiles,
        IBlobStorageService blobStorage, CancellationToken ct)
    {
        Debug.WriteLine($"[keepetl] Step 2: Processing and ingesting {totalFiles} files for ImportId: {report.ImportId}");
        logger.LogInformation("Step 2: Processing and ingesting files for ImportId: {ImportId}", report.ImportId);

        var totals = new IngestionTotals();
        var processedFileCount = 0;

        foreach (var fileSet in fileSets)
        {
            Debug.WriteLine($"[keepetl] Processing file set: {fileSet.Definition.Name} with {fileSet.Files.Length} file(s)");
            logger.LogDebug("Processing file set for definition: {DefinitionName} with {FileCount} file(s) for ImportId: {ImportId}", fileSet.Definition.Name, fileSet.Files.Length, report.ImportId);

            var orderedFiles = fileSet.Files.OrderBy(f => f.Timestamp).ToArray();

            foreach (var file in orderedFiles)
            {
                processedFileCount++;

                var fileResult = await ProcessSingleFileAsync(report.ImportId, fileSet, file, processedFileCount, totalFiles, blobStorage, report, ct);

                totals = totals.Add(fileResult);

                // Clear current file status after completion and update overall progress
                report.IngestionPhase!.FilesProcessed = processedFileCount;
                report.IngestionPhase!.FilesSkipped = totals.FilesSkipped;
                report.IngestionPhase!.RecordsCreated = totals.RecordsCreated;
                report.IngestionPhase!.RecordsUpdated = totals.RecordsUpdated;
                report.IngestionPhase!.RecordsDeleted = totals.RecordsDeleted;
                report.IngestionPhase!.CurrentFileStatus = null;
                await reportingService.UpsertImportReportAsync(report, ct);
            }
        }

        Debug.WriteLine($"[keepetl] Step 2 completed: Processed {processedFileCount} file(s) for ImportId: {report.ImportId}");
        logger.LogInformation("Step 2 completed: Processed {ProcessedFileCount} file(s) for ImportId: {ImportId}",
            processedFileCount,
            report.ImportId);

        return totals with { FilesProcessed = processedFileCount };
    }

    private async Task<IngestionTotals> ProcessSingleFileAsync(Guid importId, FileSet fileSet, EtlFile file, int currentFileNumber,
        int totalFiles, IBlobStorageService blobStorage, ImportReport report, CancellationToken ct)
    {
        var fileStopwatch = Stopwatch.StartNew();

        Debug.WriteLine($"[keepetl] Processing file {currentFileNumber}/{totalFiles}: {file.StorageObject.Key}");
        logger.LogInformation("Processing file {CurrentFile}/{TotalFiles}: {FileKey} for ImportId: {ImportId}", currentFileNumber, totalFiles, file.StorageObject.Key, importId);

        try
        {
            // Check if file has already been ingested by retrieving ETag from S3 metadata
            var isAlreadyIngested = await IsFileAlreadyIngestedAsync(file.StorageObject.Key, blobStorage, ct);
            if (isAlreadyIngested)
            {
                fileStopwatch.Stop();
                Debug.WriteLine($"[keepetl] Skipping file {file.StorageObject.Key} - already ingested in a previous import");
                logger.LogInformation("Skipping file {FileKey} - already ingested in a previous import for ImportId: {ImportId}", file.StorageObject.Key, importId);

                return new IngestionTotals()
                {
                    FilesSkipped = 1
                };
            }

            var fileMetrics = await IngestFileAsync(importId, blobStorage, fileSet, file, report, ct);
            fileStopwatch.Stop();

            await RecordSuccessfulIngestionAsync(importId, file, fileMetrics, fileStopwatch.ElapsedMilliseconds, ct);

            Debug.WriteLine($"[keepetl] Successfully ingested file: {file.StorageObject.Key} - Created: {fileMetrics.RecordsCreated}, Updated: {fileMetrics.RecordsUpdated}, Deleted: {fileMetrics.RecordsDeleted}, Duration: {fileStopwatch.ElapsedMilliseconds}ms");
            logger.LogInformation("Successfully ingested file: {FileKey} - Created: {Created}, Updated: {Updated}, Deleted: {Deleted}, Duration: {Duration}ms",
                file.StorageObject.Key, fileMetrics.RecordsCreated, fileMetrics.RecordsUpdated, fileMetrics.RecordsDeleted, fileStopwatch.ElapsedMilliseconds);

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
            Debug.WriteLine($"[keepetl] Failed to ingest file: {file.StorageObject.Key} after {fileStopwatch.ElapsedMilliseconds}ms - Error: {ex.Message}");
            logger.LogError(ex, "Failed to ingest file: {FileKey} after {Duration}ms for ImportId: {ImportId}",
                file.StorageObject.Key,
                fileStopwatch.ElapsedMilliseconds,
                importId);

            await RecordFailedIngestionAsync(importId, file, fileStopwatch.ElapsedMilliseconds, ex, ct);

            throw;
        }
    }

    private async Task<bool> IsFileAlreadyIngestedAsync(string fileKey, IBlobStorageService blobStorage, CancellationToken ct)
    {
        try
        {
            // Retrieve file metadata from S3 to get the ETag
            var metadata = await blobStorage.GetMetadataAsync(fileKey, ct);

            if (string.IsNullOrEmpty(metadata.ETag))
            {
                Debug.WriteLine($"[keepetl] No ETag found for file {fileKey} - will proceed with ingestion");
                logger.LogInformation("No ETag found for file {FileKey} - will proceed with ingestion", fileKey);
                return false;
            }

            // Check if a file with this key and ETag has been previously ingested (not just acquired)
            var isIngested = await reportingService.IsFileIngestedAsync(fileKey, metadata.ETag, ct);

            if (isIngested)
            {
                Debug.WriteLine($"[keepetl] File {fileKey} with ETag {metadata.ETag} has already been ingested");
                logger.LogInformation("File {FileKey} with ETag {ETag} has already been ingested in a previous import", fileKey, metadata.ETag);
            }
            else
            {
                logger.LogInformation("File {FileKey} with ETag {ETag} has NOT been ingested in a previously.", fileKey, metadata.ETag);
            }

            return isIngested;
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"[keepetl] Error checking if file was already ingested: {fileKey} - Error: {ex.Message}");
            logger.LogWarning(ex, "Error checking if file {FileKey} was already ingested - will proceed with ingestion", fileKey);
            return false;
        }
    }

    private async Task<FileIngestionMetrics> IngestFileAsync(Guid importId, IBlobStorageService blobs, FileSet fileSet, EtlFile file, ImportReport report, CancellationToken ct)
    {
        var overallStopwatch = Stopwatch.StartNew();
        var collectionName = fileSet.Definition.Name;

        Debug.WriteLine($"[keepetl] Starting ingestion of file {file.StorageObject.Key} into collection {collectionName}");
        logger.LogInformation("Starting ingestion of file {FileKey} into collection {CollectionName}",
            file.StorageObject.Key, collectionName);

        metrics.RecordCount(MetricNames.File, 1,
            (MetricNames.CommonTags.Operation, MetricNames.Operations.FileIngestionStarted),
            (MetricNames.CommonTags.Collection, collectionName));

        var collection = await EnsureCollectionExistsAsync(collectionName, ct);
        await EnsureWildcardIndexExistsAsync(collection, ct);

        string? tempFilePath = null;
        IngestionProgressTracker? progressTracker = null;

        try
        {
            // Track S3 download time
            var downloadStopwatch = Stopwatch.StartNew();
            tempFilePath = await DownloadToTempFileAsync(blobs, file.StorageObject.Key, ct);
            downloadStopwatch.Stop();

            Debug.WriteLine($"[keepetl] Downloaded file {file.StorageObject.Key} to temp storage: {tempFilePath} in {downloadStopwatch.ElapsedMilliseconds}ms");
            logger.LogInformation("Downloaded file {FileKey} to temp storage: {TempPath} in {DownloadDuration}ms",
                file.StorageObject.Key, tempFilePath, downloadStopwatch.ElapsedMilliseconds);

            // Count rows for progress tracking
            var estimatedRowCount = await _rowCounter.CountRowsAsync(tempFilePath, ct);
            progressTracker = new IngestionProgressTracker(file.StorageObject.Key, estimatedRowCount);

            Debug.WriteLine($"[keepetl] File {file.StorageObject.Key} has approximately {estimatedRowCount} data rows to process");
            logger.LogInformation("File {FileKey} has approximately {RowCount} data rows to process",
                file.StorageObject.Key, estimatedRowCount);

            // Track MongoDB ingestion time
            var mongoIngestionStopwatch = Stopwatch.StartNew();

            var csvContext = await OpenCsvFileFromDiskAsync(tempFilePath, ct);

            var headers = await ReadAndValidateHeadersAsync(
                csvContext.Csv,
                file.StorageObject.Key,
                fileSet.Definition.PrimaryKeyHeaderNames,
                fileSet.Definition.ChangeTypeHeaderName);

            var fileMetrics = await ProcessCsvRecordsAsync(importId, collection, csvContext.Csv, headers, file.StorageObject.Key,
                collectionName, fileSet.Definition, progressTracker, report, ct);

            await csvContext.DisposeAsync();

            mongoIngestionStopwatch.Stop();
            overallStopwatch.Stop();

            Debug.WriteLine($"[keepetl] Completed ingestion of file {file.StorageObject.Key}. Total records: {fileMetrics.RecordsProcessed}, Created: {fileMetrics.RecordsCreated}, Updated: {fileMetrics.RecordsUpdated}, Deleted: {fileMetrics.RecordsDeleted}, S3 Download: {downloadStopwatch.ElapsedMilliseconds}ms, MongoDB Ingestion: {mongoIngestionStopwatch.ElapsedMilliseconds}ms, Total Duration: {overallStopwatch.ElapsedMilliseconds}ms, Avg Record Processing: {fileMetrics.AverageMongoIngestionMs:F2}ms/record");
            logger.LogInformation("Completed ingestion of file {FileKey}. Total records: {TotalRecords}, Created: {Created}, Updated: {Updated}, Deleted: {Deleted}, S3 Download: {DownloadDuration}ms, MongoDB Ingestion: {MongoIngestionDuration}ms, Total Duration: {TotalDuration}ms, Avg Record Processing: {AvgMs:F2}ms/record",
                file.StorageObject.Key,
                fileMetrics.RecordsProcessed,
                fileMetrics.RecordsCreated,
                fileMetrics.RecordsUpdated,
                fileMetrics.RecordsDeleted,
                downloadStopwatch.ElapsedMilliseconds,
                mongoIngestionStopwatch.ElapsedMilliseconds,
                overallStopwatch.ElapsedMilliseconds,
                fileMetrics.AverageMongoIngestionMs);

            var result = fileMetrics with
            {
                S3DownloadDurationMs = downloadStopwatch.ElapsedMilliseconds,
                MongoIngestionDurationMs = mongoIngestionStopwatch.ElapsedMilliseconds
            };

            metrics.RecordRequest(MetricNames.File, MetricNames.Operations.FileIngested);
            metrics.RecordCount(MetricNames.File, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.FileIngested),
                (MetricNames.CommonTags.Collection, collectionName),
                (MetricNames.CommonTags.Status, "success"));

            metrics.RecordCount(MetricNames.File, fileMetrics.RecordsProcessed,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.FileRecordsProcessed),
                (MetricNames.CommonTags.Collection, collectionName));
            metrics.RecordValue(MetricNames.File, downloadStopwatch.ElapsedMilliseconds,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.FileS3Download),
                (MetricNames.CommonTags.Collection, collectionName));
            metrics.RecordValue(MetricNames.File, mongoIngestionStopwatch.ElapsedMilliseconds,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.FileMongoIngestion),
                (MetricNames.CommonTags.Collection, collectionName));
            metrics.RecordValue(MetricNames.File, fileMetrics.AverageMongoIngestionMs,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.FileAvgRecordProcessing),
                (MetricNames.CommonTags.Collection, collectionName));

            if (overallStopwatch.ElapsedMilliseconds > 0)
            {
                var s3Ratio = (double)downloadStopwatch.ElapsedMilliseconds / overallStopwatch.ElapsedMilliseconds;
                var mongoRatio = (double)mongoIngestionStopwatch.ElapsedMilliseconds / overallStopwatch.ElapsedMilliseconds;

                metrics.RecordValue(MetricNames.File, s3Ratio,
                    (MetricNames.CommonTags.Operation, MetricNames.Operations.FileS3Ratio),
                    (MetricNames.CommonTags.Collection, collectionName));
                metrics.RecordValue(MetricNames.File, mongoRatio,
                    (MetricNames.CommonTags.Operation, MetricNames.Operations.FileMongoRatio),
                    (MetricNames.CommonTags.Collection, collectionName));
            }

            return result;
        }
        finally
        {
            // Ensure temp file is always cleaned up
            if (tempFilePath != null && File.Exists(tempFilePath))
            {
                try
                {
                    File.Delete(tempFilePath);
                    Debug.WriteLine($"[keepetl] Deleted temp file: {tempFilePath}");
                    logger.LogDebug("Deleted temp file: {TempPath}", tempFilePath);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"[keepetl] Failed to delete temp file: {tempFilePath} - Error: {ex.Message}");
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

        Debug.WriteLine($"[keepetl] Downloading {fileKey} to {tempFilePath}");
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
        Debug.WriteLine($"[keepetl] Opening CSV file from disk: {filePath}");
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

        return await Task.FromResult(new CsvContext(stream, reader, csv));
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
            Debug.WriteLine($"[keepetl] No headers found in file {fileKey}, skipping ingestion");
            logger.LogWarning("No headers found in file {FileKey}, skipping ingestion", fileKey);
            throw new InvalidOperationException($"No headers found in file {fileKey}");
        }

        // Normalize headers by removing surrounding quotes
        var headers = NormalizeHeaders(rawHeaders);

        ValidatePrimaryKeyHeaders(headers, primaryKeyHeaderNames);
        ValidateChangeTypeHeader(headers, changeTypeHeaderName);

        Debug.WriteLine($"[keepetl] CSV headers read successfully. Total columns: {headers.Length}, Primary Keys: {string.Join(", ", primaryKeyHeaderNames)}, Change Type: {changeTypeHeaderName}");
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


    private async Task<FileIngestionMetrics> ProcessCsvRecordsAsync(Guid importId, IMongoCollection<BsonDocument> collection, CsvReader csv,
        CsvHeaders headers, string fileKey, string collectionName, DataSetDefinition definition, IngestionProgressTracker progressTracker,
        ImportReport report, CancellationToken ct)
    {
        Debug.WriteLine($"[keepetl] Starting to process CSV records for file: {fileKey}");
        var fileMetrics = new FileMetricsTracker();
        var batch = new List<(BsonDocument Document, string ChangeType)>();
        var lineageEvents = new List<RecordLineageEvent>();
        var totalMongoProcessingMs = 0L;
        var totals = new IngestionTotals();
        var lastReportedRecordCount = 0;

        while (await csv.ReadAsync())
        {
            var changeType = csv.GetField(headers.ChangeTypeHeaderName)?.ToUpperInvariant() ?? string.Empty;

            if (!IsValidChangeType(changeType))
            {
                var primaryKeyValue = string.Join(EtlConstants.CompositeKeyDelimiter, headers.PrimaryKeyHeaderNames.Select(pkHeader => csv.GetField(pkHeader) ?? string.Empty));
                Debug.WriteLine($"[keepetl] Invalid change type '{changeType}' for record with primary key '{primaryKeyValue}' in file {fileKey}, skipping record");
                logger.LogWarning("Invalid change type '{ChangeType}' for record with primary key '{PrimaryKey}' in file {FileKey}, skipping record",
                    changeType, primaryKeyValue, fileKey);
                fileMetrics.RecordsSkipped++;
                continue;
            }

            var document = CreateDocumentFromCsvRecord(csv, headers, definition);
            batch.Add((document, changeType));

            if (batch.Count >= BatchSize)
            {
                var batchStopwatch = Stopwatch.StartNew();
                Debug.WriteLine($"[keepetl] -- NEW BATCH (size:{BatchSize}) --");
                Debug.WriteLine($"[keepetl] Processing batch of {batch.Count} documents for collection {collectionName}");

                var batchMetrics = await ProcessBatchAsync(importId, collection, batch, fileKey, collectionName, definition, lineageEvents, ct);

                await FlushLineageEventsAsync(lineageEvents, ct);

                var batchDto = new KeeperData.Core.Reporting.Dtos.BatchProcessingMetrics
                {
                    RecordsProcessed = batchMetrics.RecordsProcessed,
                    RecordsCreated = batchMetrics.RecordsCreated,
                    RecordsUpdated = batchMetrics.RecordsUpdated,
                    RecordsDeleted = batchMetrics.RecordsDeleted
                };
                fileMetrics.AddBatch(batchDto);
                totals = totals.Add(new IngestionTotals
                {
                    RecordsCreated = batchMetrics.RecordsCreated,
                    RecordsUpdated = batchMetrics.RecordsUpdated,
                    RecordsDeleted = batchMetrics.RecordsDeleted
                });

                batchStopwatch.Stop();
                totalMongoProcessingMs += batchStopwatch.ElapsedMilliseconds;

                Debug.WriteLine($"[keepetl] Processed batch of {BatchSize} records from {fileKey} in {batchStopwatch.ElapsedMilliseconds}ms. Total processed: {fileMetrics.RecordsProcessed}, Created: {fileMetrics.RecordsCreated}, Updated: {fileMetrics.RecordsUpdated}, Deleted: {fileMetrics.RecordsDeleted}");
                Debug.WriteLine($"[keepetl] -- END BATCH ({batchStopwatch.Elapsed.TotalSeconds}s, {batchStopwatch.ElapsedMilliseconds}ms) --");

                if (BatchDelayMs > 0)
                {
                    Debug.WriteLine($"[keepetl] Throttling: waiting {BatchDelayMs}ms before next batch");
                    await Task.Delay(BatchDelayMs, ct);
                }

                Debug.WriteLine($"[keepetl] ");

                batch.Clear();
            }

            if (fileMetrics.RecordsProcessed > lastReportedRecordCount &&
                fileMetrics.RecordsProcessed - lastReportedRecordCount >= ProgressUpdateInterval)
            {
                LogProgressIfNeeded(fileMetrics.RecordsProcessed, fileKey);
                progressTracker.UpdateProgress(fileMetrics.RecordsProcessed);

                var currentStatus = progressTracker.GetCurrentStatus();
                // Update report with current file status
                report.IngestionPhase!.RecordsCreated = totals.RecordsCreated + fileMetrics.RecordsCreated;
                report.IngestionPhase!.RecordsUpdated = totals.RecordsUpdated + fileMetrics.RecordsUpdated;
                report.IngestionPhase!.RecordsDeleted = totals.RecordsDeleted + fileMetrics.RecordsDeleted;
                report.IngestionPhase!.CurrentFileStatus = new IngestionCurrentFileStatus
                {
                    FileName = currentStatus.FileName,
                    TotalRows = currentStatus.TotalRows,
                    RowNumber = currentStatus.RowNumber,
                    PercentageCompleted = currentStatus.PercentageCompleted,
                    RowsPerMinute = currentStatus.RowsPerMinute,
                    EstimatedTimeRemaining = currentStatus.EstimatedTimeRemaining,
                    EstimatedCompletionUtc = currentStatus.EstimatedCompletionUtc
                };
                await reportingService.UpsertImportReportAsync(report, ct);

                Debug.WriteLine($"[keepetl] STATS: rpm:{currentStatus.RowsPerMinute}, {currentStatus.PercentageCompleted}% done, tot:{currentStatus.TotalRows}, num:{currentStatus.RowNumber}");

                lastReportedRecordCount = fileMetrics.RecordsProcessed;
            }

        } // [end while]


        // Process remaining records
        if (batch.Count > 0)
        {
            Debug.WriteLine($"[keepetl] Processing final batch of {batch.Count} records from {fileKey}");
            var batchMetrics = await ProcessBatchAsync(
                importId,
                collection,
                batch,
                fileKey,
                collectionName,
                definition,
                lineageEvents,
                ct);

            var batchDto = new KeeperData.Core.Reporting.Dtos.BatchProcessingMetrics
            {
                RecordsProcessed = batchMetrics.RecordsProcessed,
                RecordsCreated = batchMetrics.RecordsCreated,
                RecordsUpdated = batchMetrics.RecordsUpdated,
                RecordsDeleted = batchMetrics.RecordsDeleted
            };
            fileMetrics.AddBatch(batchDto);
        }

        // Final progress update
        progressTracker.UpdateProgress(fileMetrics.RecordsProcessed);
        var finalStatus = progressTracker.Complete();

        report.IngestionPhase!.RecordsCreated = totals.RecordsCreated + fileMetrics.RecordsCreated;
        report.IngestionPhase!.RecordsUpdated = totals.RecordsUpdated + fileMetrics.RecordsUpdated;
        report.IngestionPhase!.RecordsDeleted = totals.RecordsDeleted + fileMetrics.RecordsDeleted;
        report.IngestionPhase!.CurrentFileStatus = new IngestionCurrentFileStatus
        {
            FileName = finalStatus.FileName,
            TotalRows = finalStatus.TotalRows,
            RowNumber = finalStatus.RowNumber,
            PercentageCompleted = finalStatus.PercentageCompleted,
            RowsPerMinute = finalStatus.RowsPerMinute,
            EstimatedTimeRemaining = finalStatus.EstimatedTimeRemaining,
            EstimatedCompletionUtc = finalStatus.EstimatedCompletionUtc
        };
        await reportingService.UpsertImportReportAsync(report, ct);

        // Flush remaining lineage events
        await FlushLineageEventsAsync(lineageEvents, ct);

        // Calculate average MongoDB ingestion time per record
        var avgMongoMs = fileMetrics.RecordsProcessed > 0 ? (double)totalMongoProcessingMs / fileMetrics.RecordsProcessed : 0;

        Debug.WriteLine($"[keepetl] Finished processing CSV records for file: {fileKey}. Total: {fileMetrics.RecordsProcessed}, Created: {fileMetrics.RecordsCreated}, Updated: {fileMetrics.RecordsUpdated}, Deleted: {fileMetrics.RecordsDeleted}, Avg: {avgMongoMs:F2}ms/record");

        return new FileIngestionMetrics
        {
            RecordsProcessed = fileMetrics.RecordsProcessed,
            RecordsCreated = fileMetrics.RecordsCreated,
            RecordsUpdated = fileMetrics.RecordsUpdated,
            RecordsDeleted = fileMetrics.RecordsDeleted,
            AverageMongoIngestionMs = avgMongoMs,
            S3DownloadDurationMs = 0,
            MongoIngestionDurationMs = totalMongoProcessingMs
        };
    }

    private static bool IsValidChangeType(string changeType)
    {
        return changeType == ChangeType.Delete ||
               changeType == ChangeType.Update ||
               changeType == ChangeType.Insert;
    }

    private void LogProgressIfNeeded(int recordsProcessed, string fileKey)
    {
        if (recordsProcessed % (LogInterval * BatchSize) == 0 || recordsProcessed % LogInterval == 0)
        {
            Debug.WriteLine($"[keepetl] Imported {recordsProcessed} records from file {fileKey}");
            logger.LogInformation("Imported {RecordsProcessed} records from file {FileKey}",
                recordsProcessed, fileKey);
        }
    }

    private async Task FlushLineageEventsAsync(List<RecordLineageEvent> lineageEvents, CancellationToken ct)
    {
        if (lineageEvents.Count > 0)
        {
            var flushStopwatch = Stopwatch.StartNew();
            Debug.WriteLine($"[keepetl] Flushing {lineageEvents.Count} lineage events");
            await reportingService.RecordLineageEventsBatchAsync(lineageEvents, ct);
            flushStopwatch.Stop();
            var elapsedSeconds = flushStopwatch.Elapsed.TotalSeconds;
            Debug.WriteLine($"[keepetl] Lineage events flushed in {elapsedSeconds:F3}s");
            lineageEvents.Clear();
        }
    }

    private async Task<IMongoCollection<BsonDocument>> EnsureCollectionExistsAsync(string collectionName, CancellationToken ct)
    {
        var database = mongoClient.GetDatabase(_databaseConfig.DatabaseName);

        var collectionList = await _resilientMongoOps.ListCollectionNamesAsync(database, ct);

        if (!collectionList.Contains(collectionName))
        {
            Debug.WriteLine($"[keepetl] Creating collection {collectionName}");
            logger.LogInformation("Creating collection {CollectionName}", collectionName);
            await _resilientMongoOps.CreateCollectionAsync(database, collectionName, ct);
        }
        else
        {
            Debug.WriteLine($"[keepetl] Collection {collectionName} already exists");
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
                Debug.WriteLine($"[keepetl] Wildcard index already exists on collection {collection.CollectionNamespace.CollectionName}");
                logger.LogDebug("Wildcard index already exists on collection {CollectionName}",
                    collection.CollectionNamespace.CollectionName);
                return;
            }

            await CreateWildcardIndexAsync(collection, ct);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"[keepetl] Failed to create wildcard index on collection {collection.CollectionNamespace.CollectionName}. Continuing with ingestion. Error: {ex.Message}");
            logger.LogWarning(ex, "Failed to create wildcard index on collection {CollectionName}. Continuing with ingestion.",
                collection.CollectionNamespace.CollectionName);
        }
    }

    private async Task<bool> WildcardIndexExistsAsync(
        IMongoCollection<BsonDocument> collection,
        CancellationToken ct)
    {
        var indexList = await _resilientMongoOps.ListIndexesAsync(collection, ct);

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
        Debug.WriteLine($"[keepetl] Creating wildcard index on collection {collection.CollectionNamespace.CollectionName}");
        logger.LogInformation("Creating wildcard index on collection {CollectionName}",
            collection.CollectionNamespace.CollectionName);

        var wildcardIndexKeys = Builders<BsonDocument>.IndexKeys.Wildcard();
        var indexModel = new CreateIndexModel<BsonDocument>(wildcardIndexKeys);

        await _resilientMongoOps.CreateIndexAsync(collection, indexModel, ct);

        Debug.WriteLine($"[keepetl] Wildcard index created successfully on collection {collection.CollectionNamespace.CollectionName}");
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

        // Create URL-safe composite primary key for _id using RecordIdGenerator
        var compositeKeyParts = headers.PrimaryKeyHeaderNames.Select(pkHeader => csv.GetField(pkHeader) ?? string.Empty);
        document[FieldId] = _recordIdGenerator.GenerateId(compositeKeyParts);

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

        var batchStopwatch = Stopwatch.StartNew();

        metrics.RecordCount(MetricNames.Batch, 1,
            (MetricNames.CommonTags.Operation, MetricNames.Operations.BatchStarted),
            (MetricNames.CommonTags.Collection, collectionName),
            ("batch_size", batch.Count.ToString()));

        var documentIds = batch.Select(b => b.Document[FieldId]).ToList();
        var existingDocsDict = await FetchExistingDocumentsAsync(collection, documentIds, ct);
        var softDeletedIds = GetSoftDeletedIds(existingDocsDict);

        var bulkOps = new List<WriteModel<BsonDocument>>();
        var batchMetrics = new BatchMetricsTracker();

        var insertOps = batch.Count(b => b.ChangeType == ChangeType.Insert);
        var updateOps = batch.Count(b => b.ChangeType == ChangeType.Update);
        var deleteOps = batch.Count(b => b.ChangeType == ChangeType.Delete);

        foreach (var (document, changeType) in batch)
        {
            var docId = document[FieldId];
            var recordId = docId.ToString() ?? string.Empty;
            var existingDoc = existingDocsDict.GetValueOrDefault(docId);

            if (changeType == ChangeType.Delete)
            {
                ProcessDeleteOperation(bulkOps, lineageEvents, docId, recordId,
                    existingDoc, importId, fileKey, collectionName, changeType);

                batchMetrics.Deleted++;
                batchMetrics.Processed++;
            }
            else if (changeType == ChangeType.Insert || changeType == ChangeType.Update)
            {
                var isSoftDeleted = softDeletedIds.Contains(docId);
                var isCreate = existingDoc == null;

                ProcessUpsertOperation(bulkOps, lineageEvents, document, docId, recordId, existingDoc, isCreate, isSoftDeleted,
                    importId, fileKey, collectionName, changeType, definition);

                if (isCreate)
                {
                    batchMetrics.Created++;
                }
                else
                {
                    batchMetrics.Updated++;
                }
                batchMetrics.Processed++;
            }
        }

        if (bulkOps.Count > 0)
        {
            Debug.WriteLine($"[keepetl] Executing bulk write of {bulkOps.Count} operations for collection {collectionName}");
            await _resilientMongoOps.BulkWriteAsync(collection, bulkOps, new BulkWriteOptions { IsOrdered = false }, ct);
        }

        batchStopwatch.Stop();
        var elapsedSeconds = batchStopwatch.Elapsed.TotalSeconds;

        Debug.WriteLine($"[keepetl] Batch processing complete. Processed: {batchMetrics.Processed}, Created: {batchMetrics.Created}, Updated: {batchMetrics.Updated}, Deleted: {batchMetrics.Deleted}, Elapsed: {elapsedSeconds:F3}s");

        metrics.RecordValue(MetricNames.Batch, batchStopwatch.ElapsedMilliseconds,
            (MetricNames.CommonTags.Operation, MetricNames.Operations.BatchDuration),
            ("batch_size", batch.Count.ToString()),
            (MetricNames.CommonTags.Collection, collectionName));

        if (batchStopwatch.ElapsedMilliseconds > 0)
        {
            var recordsPerSecond = (batch.Count * 1000.0) / batchStopwatch.ElapsedMilliseconds;
            metrics.RecordValue(MetricNames.Batch, recordsPerSecond,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.BatchRecordsPerSecond),
                (MetricNames.CommonTags.Collection, collectionName));
        }

        metrics.RecordCount(MetricNames.Batch, insertOps,
            (MetricNames.CommonTags.Operation, MetricNames.Operations.BatchInserts),
            (MetricNames.CommonTags.Collection, collectionName));
        metrics.RecordCount(MetricNames.Batch, updateOps,
            (MetricNames.CommonTags.Operation, MetricNames.Operations.BatchUpdates),
            (MetricNames.CommonTags.Collection, collectionName));
        metrics.RecordCount(MetricNames.Batch, deleteOps,
            (MetricNames.CommonTags.Operation, MetricNames.Operations.BatchDeletes),
            (MetricNames.CommonTags.Collection, collectionName));

        metrics.RecordRequest(MetricNames.Batch, "completed");

        return new BatchProcessingMetrics
        {
            RecordsProcessed = batchMetrics.Processed,
            RecordsCreated = batchMetrics.Created,
            RecordsUpdated = batchMetrics.Updated,
            RecordsDeleted = batchMetrics.Deleted
        };
    }

    private async Task<Dictionary<BsonValue, BsonDocument>> FetchExistingDocumentsAsync(
        IMongoCollection<BsonDocument> collection,
     List<BsonValue> documentIds,
      CancellationToken ct)
    {
        var filter = Builders<BsonDocument>.Filter.In(FieldId, documentIds);
        return await _resilientMongoOps.FindAndMapAsync(collection, filter, ct);
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
            Debug.WriteLine($"[keepetl] Undeleting soft-deleted record with _id: {docId} in file {fileKey}");
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
        EtlFile file,
        FileIngestionMetrics metrics,
        long durationMs,
        CancellationToken ct)
    {
        await reportingService.RecordFileIngestionAsync(importId, new FileIngestionRecord
        {
            FileKey = file.StorageObject.Key,
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
        EtlFile file,
        long durationMs,
        Exception ex,
        CancellationToken ct)
    {
        try
        {
            await reportingService.RecordFileIngestionAsync(importId, new FileIngestionRecord
            {
                FileKey = file.StorageObject.Key,
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
            logger.LogError(reportEx, "Failed to record ingestion failure for file: {FileKey}", file.StorageObject.Key);
        }
    }

    private void LogPipelineCompletion(Guid importId, Stopwatch stopwatch)
    {
        stopwatch.Stop();
        Debug.WriteLine($"[keepetl] Import pipeline completed successfully for ImportId: {importId}. Total duration: {stopwatch.ElapsedMilliseconds}ms ({stopwatch.Elapsed.TotalSeconds}s)");
        logger.LogInformation("Import pipeline completed successfully for ImportId: {ImportId}. Total duration: {Duration}ms ({DurationSeconds}s)",
            importId,
            stopwatch.ElapsedMilliseconds,
            stopwatch.Elapsed.TotalSeconds);
    }

    private void LogPipelineFailure(Guid importId, Stopwatch stopwatch, Exception ex)
    {
        stopwatch.Stop();
        Debug.WriteLine($"[keepetl] Import pipeline failed for ImportId: {importId} after {stopwatch.ElapsedMilliseconds}ms ({stopwatch.Elapsed.TotalSeconds}s) - Error: {ex.Message}");
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
        public int FilesSkipped { get; init; }
        public int RecordsCreated { get; init; }
        public int RecordsUpdated { get; init; }
        public int RecordsDeleted { get; init; }

        public IngestionTotals Add(IngestionTotals other) => new()
        {
            FilesProcessed = FilesProcessed + other.FilesProcessed,
            FilesSkipped = FilesSkipped + other.FilesSkipped,
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