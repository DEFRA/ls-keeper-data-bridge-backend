using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Dtos;

namespace KeeperData.Core.Reports.Abstract;

/// <summary>
/// Repository for managing cleanse analysis operations.
/// </summary>
public interface ICleanseAnalysisRepository
{
    /// <summary>
    /// Creates a new analysis operation.
    /// </summary>
    Task CreateOperationAsync(CleanseAnalysisOperation operation, CancellationToken ct = default);

    /// <summary>
    /// Updates the progress of an analysis operation.
    /// </summary>
    Task UpdateProgressAsync(
        string operationId,
        double progressPercentage,
        string statusDescription,
        int recordsAnalyzed,
        int issuesFound,
        int issuesResolved,
        CancellationToken ct = default);

    /// <summary>
    /// Marks an operation as completed.
    /// </summary>
    Task CompleteOperationAsync(
        string operationId,
        int recordsAnalyzed,
        int issuesFound,
        int issuesResolved,
        long durationMs,
        CancellationToken ct = default);

    /// <summary>
    /// Marks an operation as failed.
    /// </summary>
    Task FailOperationAsync(string operationId, string error, long durationMs, CancellationToken ct = default);

    /// <summary>
    /// Gets an operation by its identifier.
    /// </summary>
    Task<CleanseAnalysisOperation?> GetOperationAsync(string operationId, CancellationToken ct = default);

    /// <summary>
    /// Gets operations with pagination, ordered by most recent first.
    /// </summary>
    Task<IReadOnlyList<CleanseAnalysisOperationSummary>> GetOperationsAsync(int skip, int top, CancellationToken ct = default);

    /// <summary>
    /// Gets the currently running operation, if any.
    /// </summary>
    Task<CleanseAnalysisOperation?> GetCurrentOperationAsync(CancellationToken ct = default);

    /// <summary>
    /// Sets the report details for a completed operation.
    /// </summary>
    Task SetReportDetailsAsync(string operationId, string objectKey, string reportUrl, CancellationToken ct = default);

    /// <summary>
    /// Updates just the report URL for an operation (used when regenerating presigned URLs).
    /// </summary>
    Task UpdateReportUrlAsync(string operationId, string reportUrl, CancellationToken ct = default);

    /// <summary>
    /// Deletes all analysis operations.
    /// </summary>
    /// <returns>The number of documents deleted.</returns>
    Task<long> DeleteAllAsync(CancellationToken ct = default);
}
