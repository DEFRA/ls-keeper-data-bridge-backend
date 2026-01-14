namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents metrics from processing a file during ingestion
/// </summary>
public record FileIngestionMetrics
{
    public int RecordsProcessed { get; init; }
    public int RecordsCreated { get; init; }
    public int RecordsUpdated { get; init; }
    public int RecordsDeleted { get; init; }
    public long S3DownloadDurationMs { get; init; }
    public long MongoIngestionDurationMs { get; init; }
}