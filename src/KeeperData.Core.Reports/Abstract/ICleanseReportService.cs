using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Dtos;

namespace KeeperData.Core.Reports.Abstract;

/// <summary>
/// Service for running cleanse analysis and managing analysis operations.
/// </summary>
public interface ICleanseReportService
{
    /// <summary>
    /// Starts a new cleanse analysis operation.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The created operation, or null if another operation is already running.</returns>
    Task<CleanseAnalysisOperation?> StartAnalysisAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets an analysis operation by its identifier.
    /// </summary>
    Task<CleanseAnalysisOperation?> GetOperationAsync(string operationId, CancellationToken ct = default);

    /// <summary>
    /// Gets a list of analysis operations with pagination.
    /// </summary>
    Task<IReadOnlyList<CleanseAnalysisOperationSummary>> GetOperationsAsync(int skip = 0, int top = 10, CancellationToken ct = default);

    /// <summary>
    /// Gets the currently running operation, if any.
    /// </summary>
    Task<CleanseAnalysisOperation?> GetCurrentOperationAsync(CancellationToken ct = default);
}
