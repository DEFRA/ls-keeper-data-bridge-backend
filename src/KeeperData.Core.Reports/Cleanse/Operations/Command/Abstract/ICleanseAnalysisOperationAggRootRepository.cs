using KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;

namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;

/// <summary>
/// Repository for persisting cleanse analysis operation aggregate roots.
/// </summary>
public interface ICleanseAnalysisOperationAggRootRepository
{
    /// <summary>
    /// Persists a new analysis operation.
    /// </summary>
    Task CreateAsync(CleanseAnalysisOperation operation, CancellationToken ct = default);

    /// <summary>
    /// Loads an analysis operation by its identifier.
    /// </summary>
    Task<CleanseAnalysisOperation?> GetByIdAsync(string operationId, CancellationToken ct = default);

    /// <summary>
    /// Persists the current state of an analysis operation.
    /// </summary>
    Task UpdateAsync(CleanseAnalysisOperation operation, CancellationToken ct = default);

    /// <summary>
    /// Deletes all analysis operations.
    /// </summary>
    /// <returns>The number of documents deleted.</returns>
    Task<long> DeleteAllAsync(CancellationToken ct = default);
}
