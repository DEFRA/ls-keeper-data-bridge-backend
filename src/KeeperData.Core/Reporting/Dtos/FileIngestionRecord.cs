namespace KeeperData.Core.Reporting.Dtos;

public record FileIngestionRecord
{
    public required string FileKey { get; init; }
    public int RecordsProcessed { get; init; }
    public int RecordsCreated { get; init; }
    public int RecordsUpdated { get; init; }
    public int RecordsDeleted { get; init; }
    public long IngestionDurationMs { get; init; }
    public double AverageRecordIngestionMs { get; init; }
    public long S3DownloadDurationMs { get; init; }
    public long MongoIngestionDurationMs { get; init; }
    public DateTime IngestedAtUtc { get; init; }
    public FileProcessingStatus Status { get; init; }
    public string? Error { get; init; }
}