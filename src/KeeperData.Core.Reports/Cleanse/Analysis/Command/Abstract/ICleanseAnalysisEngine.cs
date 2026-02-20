using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;

/// <summary>
/// Represents an analysis strategy that processes data and detects issues.
/// </summary>
public interface ICleanseAnalysisEngine
{
    /// <summary>
    /// Executes the analysis strategy.
    /// </summary>
    /// <param name="operationId">The identifier of the current analysis operation.</param>
    /// <param name="progressCallback">Callback for reporting progress.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Metrics collected during execution.</returns>
    Task<AnalysisMetrics> ExecuteAsync(string operationId, ProgressCallback progressCallback, CancellationToken ct);
}
