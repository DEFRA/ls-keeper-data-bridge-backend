using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;

namespace KeeperData.Core.Reports.Cleanse.Operations.Queries.Abstract;

/// <summary>
/// Service for tracking and computing live performance statistics during cleanse analysis.
/// </summary>
public interface ICleanseRunStatsService
{
    /// <summary>
    /// Records a snapshot of the current progress for sliding-window RPM calculation.
    /// Called during the analysis pump after each batch.
    /// </summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="recordsAnalyzed">The cumulative number of records analyzed at this point.</param>
    void RecordSnapshot(string operationId, int recordsAnalyzed);

    /// <summary>
    /// Calculates live performance statistics for a running operation.
    /// Returns null if the operation is not running or has no data.
    /// </summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="recordsAnalyzed">The current number of records analyzed.</param>
    /// <param name="totalRecords">The total number of records to analyze.</param>
    /// <param name="startedAtUtc">The UTC time the operation started.</param>
    /// <returns>Live stats, or null if calculation is not possible.</returns>
    CleanseRunStatsDto? CalculateStats(string operationId, int recordsAnalyzed, int totalRecords, DateTime startedAtUtc);

    /// <summary>
    /// Removes all in-memory snapshot data for a completed or failed operation.
    /// </summary>
    /// <param name="operationId">The operation identifier.</param>
    void ClearSnapshots(string operationId);
}
