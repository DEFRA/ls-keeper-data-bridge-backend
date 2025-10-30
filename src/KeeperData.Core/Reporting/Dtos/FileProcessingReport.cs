namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents the processing report for a single file in an import operation.
/// </summary>
public record FileProcessingReport
{
    /// <summary>
    /// Gets the unique identifier of the import this file belongs to.
    /// </summary>
    public required Guid ImportId { get; init; }

    /// <summary>
    /// Gets the original filename.
    /// </summary>
    public required string FileName { get; init; }

    /// <summary>
    /// Gets the unique key identifying the file in the source system.
    /// </summary>
    public required string FileKey { get; init; }

    /// <summary>
    /// Gets the name of the dataset this file corresponds to.
    /// </summary>
    public required string DatasetName { get; init; }

    /// <summary>
    /// Gets the MD5 hash of the file content for integrity verification.
    /// </summary>
    public required string Md5Hash { get; init; }

    /// <summary>
    /// Gets the file size in bytes.
    /// </summary>
    public long FileSize { get; init; }

    /// <summary>
    /// Gets the current processing status of the file.
    /// </summary>
    public required FileProcessingStatus Status { get; init; }

    /// <summary>
    /// Gets the acquisition phase details for this file, or null if not yet acquired.
    /// </summary>
    public AcquisitionDetails? Acquisition { get; init; }

    /// <summary>
    /// Gets the ingestion phase details for this file, or null if not yet ingested.
    /// </summary>
    public IngestionDetails? Ingestion { get; init; }

    /// <summary>
    /// Gets the error message if file processing failed, or null if successful.
    /// </summary>
    public string? Error { get; init; }
}

/// <summary>
/// Represents acquisition details for a file, including decryption metrics.
/// </summary>
public record AcquisitionDetails
{
    /// <summary>
    /// Gets the UTC timestamp when the file was acquired.
    /// </summary>
    public DateTime AcquiredAtUtc { get; init; }

    /// <summary>
    /// Gets the source key used to identify the file location.
    /// </summary>
    public required string SourceKey { get; init; }

    /// <summary>
    /// Gets the duration of the decryption process in milliseconds.
    /// </summary>
    public long DecryptionDurationMs { get; init; }
}

/// <summary>
/// Represents ingestion details for a file, including record statistics and performance metrics.
/// </summary>
public record IngestionDetails
{
    /// <summary>
    /// Gets the UTC timestamp when the file was ingested.
    /// </summary>
    public DateTime IngestedAtUtc { get; init; }

    /// <summary>
    /// Gets the total number of records processed from this file.
    /// </summary>
    public int RecordsProcessed { get; init; }

    /// <summary>
    /// Gets the number of new records created from this file.
    /// </summary>
    public int RecordsCreated { get; init; }

    /// <summary>
    /// Gets the number of existing records updated from this file.
    /// </summary>
    public int RecordsUpdated { get; init; }

    /// <summary>
    /// Gets the number of records deleted as a result of processing this file.
    /// </summary>
    public int RecordsDeleted { get; init; }

    /// <summary>
    /// Gets the duration of the ingestion process in milliseconds.
    /// </summary>
    public long IngestionDurationMs { get; init; }
}