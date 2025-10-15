namespace KeeperData.Core.Reporting.Dtos;

public record FileProcessingReport
{
    public required Guid ImportId { get; init; }
    public required string FileName { get; init; }
    public required string FileKey { get; init; }
    public required string DatasetName { get; init; }
    public required string Md5Hash { get; init; }
    public long FileSize { get; init; }
    public required FileProcessingStatus Status { get; init; }
    public AcquisitionDetails? Acquisition { get; init; }
    public IngestionDetails? Ingestion { get; init; }
    public string? Error { get; init; }
}

public record AcquisitionDetails
{
    public DateTime AcquiredAtUtc { get; init; }
    public required string SourceKey { get; init; }
    public long DecryptionDurationMs { get; init; }
}

public record IngestionDetails
{
    public DateTime IngestedAtUtc { get; init; }
    public int RecordsProcessed { get; init; }
    public int RecordsCreated { get; init; }
    public int RecordsUpdated { get; init; }
    public int RecordsDeleted { get; init; }
    public long IngestionDurationMs { get; init; }
}
