using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using KeeperData.Core.Reports.Issues.Command.Requests;

namespace KeeperData.Core.Reports.Issues.Command;

public class IssueCommandService(IIssueAggRootRepository repo, IIssueHistoryAggRootRepository historyRepo) : IIssueCommandService
{
    public async Task<IssueRecordResult> RecordIssueAsync(RecordIssueCommand command, CancellationToken ct)
    {
        var existing = await repo.GetByIdAsync(command.Thumbprint, ct);

        if (existing is null)
        {
            var (issue, history) = Issue.Create(
                command.Thumbprint,
                command.OperationId,
                command.Descriptor,
                command.Cph,
                command.CtsLidFullIdentifier,
                command.IssueContext);

            await repo.UpsertAsync(issue, ct);
            await historyRepo.AppendAsync(history, ct);
            return IssueRecordResult.Created;
        }

        var wasInactive = !existing.IsActive;
        IssueHistoryEntry historyEntry;

        if (wasInactive)
        {
            historyEntry = existing.Reactivate(command.OperationId);
            existing.ApplyContext(command.IssueContext);
        }
        else
        {
            historyEntry = existing.Touch(command.OperationId);
        }

        await repo.UpsertAsync(existing, ct);
        await historyRepo.AppendAsync(historyEntry, ct);
        return wasInactive ? IssueRecordResult.Reactivated : IssueRecordResult.NoChange;
    }

    public async Task<int> DeactivateStaleIssuesAsync(DeactivateStaleIssuesCommand command, CancellationToken ct)
        => await repo.DeactivateStaleAsync(command.OperationId, ct);

    public async Task IgnoreIssueAsync(IgnoreIssueCommand command, CancellationToken ct)
    {
        var issue = await LoadRequiredAsync(command.IssueId, ct);
        var history = issue.Ignore(command.PerformedBy);
        await repo.UpsertAsync(issue, ct);
        await historyRepo.AppendAsync(history, ct);
    }

    public async Task UnignoreIssueAsync(UnignoreIssueCommand command, CancellationToken ct)
    {
        var issue = await LoadRequiredAsync(command.IssueId, ct);
        var history = issue.Unignore(command.PerformedBy);
        await repo.UpsertAsync(issue, ct);
        await historyRepo.AppendAsync(history, ct);
    }

    public async Task UpdateResolutionStatusAsync(UpdateResolutionStatusCommand command, CancellationToken ct)
    {
        var issue = await LoadRequiredAsync(command.IssueId, ct);
        var history = issue.UpdateResolutionStatus(command.Status, command.PerformedBy);
        await repo.UpsertAsync(issue, ct);
        await historyRepo.AppendAsync(history, ct);
    }

    public async Task AssignIssueAsync(AssignIssueCommand command, CancellationToken ct)
    {
        var issue = await LoadRequiredAsync(command.IssueId, ct);
        var history = issue.Assign(command.AssignedTo, command.PerformedBy);
        await repo.UpsertAsync(issue, ct);
        await historyRepo.AppendAsync(history, ct);
    }

    public async Task UnassignIssueAsync(UnassignIssueCommand command, CancellationToken ct)
    {
        var issue = await LoadRequiredAsync(command.IssueId, ct);
        var history = issue.Unassign(command.PerformedBy);
        await repo.UpsertAsync(issue, ct);
        await historyRepo.AppendAsync(history, ct);
    }

    public async Task<long> DeleteAllIssuesAsync(CancellationToken ct)
        => await repo.DeleteAllAsync(ct);

    private async Task<Issue> LoadRequiredAsync(string issueId, CancellationToken ct)
        => await repo.GetByIdAsync(issueId, ct)
           ?? throw new InvalidOperationException($"Issue '{issueId}' not found.");
}

