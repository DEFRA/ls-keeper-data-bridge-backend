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
    /// <returns>The number of issues deactivated.</returns>
    Task<int> DeactivateStaleAsync(string currentOperationId, CancellationToken ct = default);

    /// <summary>
    /// Deletes all issues.
    /// </summary>
    /// <returns>The number of documents deleted.</returns>
    Task<long> DeleteAllAsync(CancellationToken ct = default);
}
