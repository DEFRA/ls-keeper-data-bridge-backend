using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Issues.Command.Requests;

namespace KeeperData.Core.Reports.Issues.Command.Abstract;

public interface IIssueCommandService
{
    /// <summary>
    /// Records an issue when a rule condition is true. Creates, reactivates, or touches the issue.
    /// </summary>
    Task<IssueRecordResult> RecordIssueAsync(RecordIssueCommand command, CancellationToken ct);

    /// <summary>
    /// Deactivates all issues that were not touched by the specified operation.
    /// </summary>
    /// <returns>The number of issues deactivated.</returns>
    Task<int> DeactivateStaleIssuesAsync(DeactivateStaleIssuesCommand command, CancellationToken ct);

    /// <summary>
    /// Flags an issue as ignored.
    /// </summary>
    Task IgnoreIssueAsync(IgnoreIssueCommand command, CancellationToken ct);

    /// <summary>
    /// Removes the ignored flag from an issue.
    /// </summary>
    Task UnignoreIssueAsync(UnignoreIssueCommand command, CancellationToken ct);

    /// <summary>
    /// Updates the resolution workflow status of an issue.
    /// </summary>
    Task UpdateResolutionStatusAsync(UpdateResolutionStatusCommand command, CancellationToken ct);

    /// <summary>
    /// Assigns an issue to a user.
    /// </summary>
    Task AssignIssueAsync(AssignIssueCommand command, CancellationToken ct);

    /// <summary>
    /// Clears the assigned user from an issue.
    /// </summary>
    Task UnassignIssueAsync(UnassignIssueCommand command, CancellationToken ct);

    /// <summary>
    /// Deletes all issues.
    /// </summary>
    /// <returns>The number of issues deleted.</returns>
    Task<long> DeleteAllIssuesAsync(CancellationToken ct);
}

