using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;

namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;

/// <summary>
/// Service for running cleanse analysis and managing analysis operations.
/// </summary>
public interface ICleanseAnalysisCommandService
{
    /// <summary>
    /// Starts a new cleanse analysis operation in the background.
    /// Returns immediately after starting the background task.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The created operation, or null if another operation is already running.</returns>
    Task<CleanseAnalysisOperationDto?> StartAnalysisAsync(CancellationToken ct = default);

    /// <summary>
    /// Runs a cleanse analysis operation synchronously on the caller thread.
    /// Exceptions are propagated to the caller. Useful for testing.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The completed operation, or null if the lock could not be acquired.</returns>
    Task<CleanseAnalysisOperationDto?> RunAnalysisAsync(CancellationToken ct = default);
}
