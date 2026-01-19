using KeeperData.Core.Reporting.Dtos;

namespace KeeperData.Core.Telemetry;

/// <summary>
/// Tracks file-level metrics for telemetry - accumulator for record processing statistics
/// </summary>
public class FileMetricsTracker
{
    public string FileName { get; set; } = string.Empty;
    public DateTime StartTime { get; set; }
    public long SizeBytes { get; set; }
    public int RecordCount { get; set; }
    public int RecordsProcessed { get; set; }
    public int RecordsSkipped { get; set; }
    public int RecordsCreated { get; set; }
    public int RecordsUpdated { get; set; }
    public int RecordsDeleted { get; set; }

    public long ElapsedMilliseconds => (long)(DateTime.UtcNow - StartTime).TotalMilliseconds;

    public void AddBatch(BatchProcessingMetrics batchMetrics)
    {
        RecordsProcessed += batchMetrics.RecordsProcessed;
        RecordsCreated += batchMetrics.RecordsCreated;
        RecordsUpdated += batchMetrics.RecordsUpdated;
        RecordsDeleted += batchMetrics.RecordsDeleted;
    }
}