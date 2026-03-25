using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Operations;

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
    /// <param name="ct">Cancellation token.</param>
    /// <param name="scope">Optional operation scope for unified progress tracking.</param>
    /// <param name="isCancellationRequested">Optional function polled to detect external cancellation requests.</param>
    /// <returns>Metrics collected during execution.</returns>
    Task<AnalysisMetrics> ExecuteAsync(string operationId, CancellationToken ct,
        OperationScope? scope = null, Func<bool>? isCancellationRequested = null);
}
