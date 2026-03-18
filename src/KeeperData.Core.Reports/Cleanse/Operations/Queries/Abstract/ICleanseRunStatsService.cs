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
    /// Records a snapshot for a specific phase's sliding-window RPM calculation.
    /// </summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="phaseName">The phase name.</param>
    /// <param name="recordsProcessed">The cumulative number of records processed in this phase.</param>
    void RecordSnapshot(string operationId, string phaseName, int recordsProcessed);

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
    /// Calculates live performance statistics for a specific phase of a running operation.
    /// </summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="phaseName">The phase name.</param>
    /// <param name="recordsProcessed">The current number of records processed in this phase.</param>
    /// <param name="totalRecords">The total number of records to process in this phase.</param>
    /// <param name="phaseStartedAtUtc">The UTC time the phase started.</param>
    /// <returns>Phase stats, or null if calculation is not possible.</returns>
    PhaseStats? CalculatePhaseStats(string operationId, string phaseName, int recordsProcessed, int totalRecords, DateTime phaseStartedAtUtc);

    /// <summary>
    /// Removes all in-memory snapshot data for a completed or failed operation.
    /// </summary>
    /// <param name="operationId">The operation identifier.</param>
    void ClearSnapshots(string operationId);
}
