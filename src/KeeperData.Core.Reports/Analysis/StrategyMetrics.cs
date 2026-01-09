namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Metrics collected during strategy execution.
/// </summary>
public sealed class StrategyMetrics
{
    /// <summary>
    /// Gets or sets the number of records analyzed.
    /// </summary>
    public int RecordsAnalyzed { get; set; }

    /// <summary>
    /// Gets or sets the number of new or reactivated issues found.
    /// </summary>
    public int IssuesFound { get; set; }

    /// <summary>
    /// Gets or sets the number of issues resolved.
    /// </summary>
    public int IssuesResolved { get; set; }
}
