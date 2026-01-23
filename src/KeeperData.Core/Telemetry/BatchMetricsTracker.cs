namespace KeeperData.Core.Telemetry;

/// <summary>
/// Tracks batch-level metrics for telemetry - accumulator for batch processing statistics
/// </summary>
public class BatchMetricsTracker
{
    public string BatchId { get; set; } = string.Empty;
    public DateTime StartTime { get; set; }
    public int ProcessedRecords { get; set; }
    public int FailedRecords { get; set; }
    public string? ErrorMessage { get; set; }

    // Additional properties required by IngestionPipeline
    public int Processed { get; set; }
    public int Created { get; set; }
    public int Updated { get; set; }
    public int Deleted { get; set; }

    public long ElapsedMilliseconds => (long)(DateTime.UtcNow - StartTime).TotalMilliseconds;
    public bool HasErrors => FailedRecords > 0 || !string.IsNullOrEmpty(ErrorMessage);
}