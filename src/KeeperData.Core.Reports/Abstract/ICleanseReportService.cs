using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Dtos;

namespace KeeperData.Core.Reports.Abstract;

/// <summary>
/// Service for running cleanse analysis and managing analysis operations.
/// </summary>
public interface ICleanseReportService
{
    /// <summary>
    /// Starts a new cleanse analysis operation in the background.
    /// Returns immediately after starting the background task.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The created operation, or null if another operation is already running.</returns>
    Task<CleanseAnalysisOperation?> StartAnalysisAsync(CancellationToken ct = default);

    /// <summary>
    /// Runs a cleanse analysis operation synchronously on the caller thread.
    /// Exceptions are propagated to the caller. Useful for testing.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The completed operation, or null if the lock could not be acquired.</returns>
    Task<CleanseAnalysisOperation?> RunAnalysisAsync(CancellationToken ct = default);

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

    /// <summary>
    /// Gets a paginated list of active cleanse issues.
    /// </summary>
    /// <param name="skip">Number of records to skip.</param>
    /// <param name="top">Maximum number of records to return.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A paginated result containing active issues.</returns>
    Task<CleanseIssuesResult> ListIssuesAsync(int skip = 0, int top = 50, CancellationToken ct = default);

    /// <summary>
    /// Deletes all cleanse report data (issues).
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The result of the delete operation.</returns>
    Task<CleanseDeleteResult> DeleteReportDataAsync(CancellationToken ct = default);

    /// <summary>
    /// Deletes all cleanse analysis metadata (operation history).
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The result of the delete operation.</returns>
    Task<CleanseDeleteResult> DeleteMetadataAsync(CancellationToken ct = default);

    /// <summary>
    /// Regenerates the presigned URL for a completed operation's report.
    /// </summary>
    /// <param name="operationId">The operation ID.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The result containing the new presigned URL.</returns>
    Task<RegenerateReportUrlResult> RegenerateReportUrlAsync(string operationId, CancellationToken ct = default);
}
