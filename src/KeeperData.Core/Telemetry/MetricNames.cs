namespace KeeperData.Core.Telemetry;

public static class MetricNames
{
    public const string MeterName = "KeeperData";

    // Main metric names - one per category
    public const string Import = "keeperdata.import";
    public const string Acquisition = "keeperdata.acquisition";
    public const string Ingestion = "keeperdata.ingestion";
    public const string File = "keeperdata.file";
    public const string Batch = "keeperdata.batch";
    public const string Api = "keeperdata.api";
    public const string Http = "keeperdata.http";
    public const string HealthCheck = "keeperdata.healthcheck";

    public static class CommonTags
    {
        public const string Service = "keeperdata.service";
        public const string HealthCheck = "keeperdata.healthcheck";
        public const string Status = "keeperdata.status";
        public const string Operation = "operation";
        public const string SourceType = "source_type";
        public const string Collection = "collection";
        public const string Endpoint = "endpoint";
        public const string ErrorType = "error_type";
        public const string StatusCode = "status_code";
        public const string MetricType = "metric_type";
    }

    // Operation values for dimensions
    public static class Operations
    {
        // Import operations
        public const string ImportRequests = "requests";
        public const string ImportCompletions = "completions";
        public const string ImportErrors = "errors";
        public const string ImportRecordsPerMinute = "records_per_minute";
        public const string ImportTotalRecords = "total_records";
        public const string ImportTotalFiles = "total_files";
        public const string ImportDuration = "duration";

        // Acquisition operations
        public const string AcquisitionCompletions = "completions";
        public const string AcquisitionErrors = "errors";
        public const string AcquisitionFilesDiscovered = "files_discovered";
        public const string AcquisitionFilesProcessed = "files_processed";
        public const string AcquisitionFilesSkipped = "files_skipped";
        public const string AcquisitionProcessingRatio = "processing_ratio";
        public const string AcquisitionDuration = "duration";
        public const string AcquisitionFileDiscovery = "file_discovery";
        public const string AcquisitionFileSets = "file_sets";
        public const string AcquisitionAvgFilesPerSet = "avg_files_per_set";

        // Ingestion operations
        public const string IngestionStarted = "started";
        public const string IngestionCompletions = "completions";
        public const string IngestionErrors = "errors";
        public const string IngestionFilesProcessed = "files_processed";
        public const string IngestionRecordsCreated = "records_created";
        public const string IngestionRecordsUpdated = "records_updated";
        public const string IngestionRecordsDeleted = "records_deleted";
        public const string IngestionRecordsPerMinute = "records_per_minute";
        public const string IngestionDuration = "duration";

        // File operations
        public const string FileIngestionStarted = "ingestion_started";
        public const string FileIngested = "ingested";
        public const string FileRecordsProcessed = "records_processed";
        public const string FileS3Download = "s3_download";
        public const string FileMongoIngestion = "mongo_ingestion";
        public const string FileAvgRecordProcessing = "avg_record_processing";
        public const string FileS3Ratio = "s3_ratio";
        public const string FileMongoRatio = "mongo_ratio";

        // Batch operations
        public const string BatchStarted = "started";
        public const string BatchDuration = "duration";
        public const string BatchRecordsPerSecond = "records_per_second";
        public const string BatchInserts = "inserts";
        public const string BatchUpdates = "updates";
        public const string BatchDeletes = "deletes";

        // API operations
        public const string ApiRequests = "requests";
        public const string ApiErrors = "errors";
        public const string ApiSuccesses = "successes";
        public const string ApiConflicts = "conflicts";
        public const string ApiSummaries = "summaries";
        public const string ApiDuration = "duration";

        // HTTP operations
        public const string HttpRequests = "requests";
        public const string HttpErrors = "errors";
        public const string HttpDuration = "duration";
        public const string HttpErrorResponse = "error_response";
        public const string HttpExceptions = "exceptions";
        public const string HttpStatusCodes = "status_codes";

        // Health check operations
        public const string HealthCheckExecuted = "executed";
    }
}