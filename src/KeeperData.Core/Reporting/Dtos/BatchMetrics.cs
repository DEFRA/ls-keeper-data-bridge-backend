namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents metrics from processing a batch of records during ingestion
/// </summary>
public record BatchIngestionMetrics
{
    public int RecordsProcessed { get; init; }
    public int RecordsCreated { get; init; }
    public int RecordsUpdated { get; init; }
    public int RecordsDeleted { get; init; }
}

/// <summary>
/// Represents comprehensive metrics for batch processing including all operations
/// </summary>
public record BatchProcessingMetrics
{
    public int RecordsProcessed { get; init; }
    public int RecordsCreated { get; init; }
    public int RecordsUpdated { get; init; }
    public int RecordsDeleted { get; init; }
}