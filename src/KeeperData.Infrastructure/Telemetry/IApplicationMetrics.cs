namespace KeeperData.Infrastructure.Telemetry;

/// <summary>
/// Provides a clean, reusable interface for recording application metrics
/// </summary>
public interface IApplicationMetrics
{
    /// <summary>
    /// Records a request metric with operation and status
    /// </summary>
    /// <param name="operation">The operation name (e.g., "health_check", "api_call")</param>
    /// <param name="status">The status (e.g., "healthy", "unhealthy", "success", "error")</param>
    void RecordRequest(string operation, string status);

    /// <summary>
    /// Records a duration metric for an operation
    /// </summary>
    /// <param name="operation">The operation name</param>
    /// <param name="durationMs">Duration in milliseconds</param>
    void RecordDuration(string operation, double durationMs);

    /// <summary>
    /// Records a counter metric with optional tags
    /// </summary>
    /// <param name="name">The metric name</param>
    /// <param name="value">The count value</param>
    /// <param name="tags">Optional tags as key-value pairs</param>
    void RecordCount(string name, int value, params (string Key, string Value)[] tags);

    /// <summary>
    /// Records a value metric (histogram) with optional tags
    /// </summary>
    /// <param name="name">The metric name</param>
    /// <param name="value">The measured value</param>
    /// <param name="tags">Optional tags as key-value pairs</param>
    void RecordValue(string name, double value, params (string Key, string Value)[] tags);
}