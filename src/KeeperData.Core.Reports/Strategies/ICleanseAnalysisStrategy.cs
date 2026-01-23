using KeeperData.Core.Reports.Analysis;

namespace KeeperData.Core.Reports.Strategies;

/// <summary>
/// Represents an analysis strategy that processes data and detects issues.
/// </summary>
public interface ICleanseAnalysisStrategy
{
    /// <summary>
    /// Gets the unique name of this strategy.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Executes the analysis strategy.
    /// </summary>
    /// <param name="context">The analysis context with cached data access.</param>
    /// <param name="issueRecorder">The issue recorder for persisting issues.</param>
    /// <param name="progressCallback">Callback for reporting progress.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Metrics collected during execution.</returns>
    Task<StrategyMetrics> ExecuteAsync(
        IAnalysisContext context,
        IIssueRecorder issueRecorder,
        ProgressCallback progressCallback,
        CancellationToken ct);
}
