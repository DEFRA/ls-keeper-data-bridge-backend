using KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

namespace KeeperData.Core.Reports.Cleanse.Operations.Command;

public class CleanseOperationCommandService(ICleanseAnalysisOperationAggRootRepository repository)
    : ICleanseOperationCommandService
{
    public async Task<string> CreateOperationAsync(CreateOperationCommand command, CancellationToken ct = default)
    {
        var operation = CleanseAnalysisOperation.Create(command.TotalRecords);
        await repository.CreateAsync(operation, ct);
        return operation.Id;
    }

    public async Task UpdateProgressAsync(UpdateProgressCommand command, CancellationToken ct = default)
    {
        var operation = await repository.GetByIdAsync(command.OperationId, ct)
            ?? throw new InvalidOperationException($"Operation '{command.OperationId}' not found.");

        operation.UpdateProgress(
            command.ProgressPercentage,
            command.StatusDescription,
            command.RecordsAnalyzed,
            command.IssuesFound,
            command.IssuesResolved);

        await repository.UpdateAsync(operation, ct);
    }

    public async Task CompleteOperationAsync(CompleteOperationCommand command, CancellationToken ct = default)
    {
        var operation = await repository.GetByIdAsync(command.OperationId, ct)
            ?? throw new InvalidOperationException($"Operation '{command.OperationId}' not found.");

        operation.Complete(command.RecordsAnalyzed, command.IssuesFound, command.IssuesResolved, command.DurationMs);

        await repository.UpdateAsync(operation, ct);
    }

    public async Task FailOperationAsync(FailOperationCommand command, CancellationToken ct = default)
    {
        var operation = await repository.GetByIdAsync(command.OperationId, ct)
            ?? throw new InvalidOperationException($"Operation '{command.OperationId}' not found.");

        operation.Fail(command.Error, command.DurationMs);

        await repository.UpdateAsync(operation, ct);
    }

    public async Task SetReportDetailsAsync(SetReportDetailsCommand command, CancellationToken ct = default)
    {
        var operation = await repository.GetByIdAsync(command.OperationId, ct)
            ?? throw new InvalidOperationException($"Operation '{command.OperationId}' not found.");

        operation.SetReportDetails(command.ObjectKey, command.ReportUrl);

        await repository.UpdateAsync(operation, ct);
    }

    public async Task UpdateReportUrlAsync(UpdateReportUrlCommand command, CancellationToken ct = default)
    {
        var operation = await repository.GetByIdAsync(command.OperationId, ct)
            ?? throw new InvalidOperationException($"Operation '{command.OperationId}' not found.");

        operation.UpdateReportUrl(command.ReportUrl);

        await repository.UpdateAsync(operation, ct);
    }

    public async Task<long> DeleteMetadataAsync(CancellationToken ct)
        => await repository.DeleteAllAsync(ct);
}
