using KeeperData.Core.Reports.Issues.Command.AggregateRoots;

namespace KeeperData.Core.Reports.Issues.Command.Abstract;

/// <summary>
/// Repository for persisting Issue aggregate roots.
/// </summary>
public interface IIssueAggRootRepository
{
    /// <summary>
    /// Loads an issue by its identifier.
    /// </summary>
    Task<Issue?> GetByIdAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Persists the current state of an issue (insert or update).
    /// </summary>
    Task UpsertAsync(Issue item, CancellationToken ct = default);

    /// <summary>
    /// Deactivates all active issues whose OperationId does not match the specified current operation.
    /// </summary>
    /// <param name="currentOperationId">The current operation identifier.</param>
    /// <param name="onBatchProcessed">Optional callback invoked after each batch with (deactivatedSoFar, totalStale).</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The number of issues deactivated.</returns>
    Task<int> DeactivateStaleAsync(string currentOperationId, Func<int, int, Task>? onBatchProcessed, CancellationToken ct = default);

    /// <summary>
    /// Deletes all issues.
    /// </summary>
    /// <returns>The number of documents deleted.</returns>
    Task<long> DeleteAllAsync(CancellationToken ct = default);
}
