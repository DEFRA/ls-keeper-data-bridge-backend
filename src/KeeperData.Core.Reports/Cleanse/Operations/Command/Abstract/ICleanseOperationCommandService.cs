using KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;

/// <summary>
/// Command service for managing cleanse analysis operation state changes.
/// </summary>
public interface ICleanseOperationCommandService
{
    /// <summary>
    /// Creates a new analysis operation and returns the operation ID.
    /// </summary>
    Task<string> CreateOperationAsync(CreateOperationCommand command, CancellationToken ct = default);

    /// <summary>
    /// Updates the progress of an analysis operation.
    /// </summary>
    Task UpdateProgressAsync(UpdateProgressCommand command, CancellationToken ct = default);

    /// <summary>
    /// Marks an operation as completed.
    /// </summary>
    Task CompleteOperationAsync(CompleteOperationCommand command, CancellationToken ct = default);

    /// <summary>
    /// Marks an operation as failed.
    /// </summary>
    Task FailOperationAsync(FailOperationCommand command, CancellationToken ct = default);

    /// <summary>
    /// Sets the report details for a completed operation.
    /// </summary>
    Task SetReportDetailsAsync(SetReportDetailsCommand command, CancellationToken ct = default);

    /// <summary>
    /// Updates just the report URL for an operation (used when regenerating presigned URLs).
    /// </summary>
    Task UpdateReportUrlAsync(UpdateReportUrlCommand command, CancellationToken ct = default);

    /// <summary>
    /// Deletes all analysis operation records.
    /// </summary>
    /// <returns>The number of records deleted.</returns>
    Task<long> DeleteMetadataAsync(CancellationToken ct);
}
