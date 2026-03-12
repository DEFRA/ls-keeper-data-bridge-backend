using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;

/// <summary>
/// Progress tracking for a single phase of a cleanse analysis operation.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO class - no logic to test.")]
public class OperationPhaseProgress
{
    /// <summary>
    /// Gets or sets the phase name (e.g., "Analysis", "Deactivation", "Export").
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the phase status: "NotStarted", "Running", or "Completed".
    /// </summary>
    public string Status { get; set; } = "NotStarted";

    /// <summary>
    /// Gets or sets the progress percentage (0-100) within this phase.
    /// </summary>
    public double Percentage { get; set; }

    /// <summary>
    /// Gets or sets a human-readable description of this phase's current progress.
    /// </summary>
    public string Description { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the number of records/units processed so far in this phase.
    /// </summary>
    public int RecordsProcessed { get; set; }

    /// <summary>
    /// Gets or sets the total number of records/units to process in this phase.
    /// </summary>
    public int TotalRecords { get; set; }

    /// <summary>
    /// Gets or sets the UTC timestamp when this phase started.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? StartedAtUtc { get; set; }

    /// <summary>
    /// Gets or sets the UTC timestamp when this phase completed.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? CompletedAtUtc { get; set; }

    /// <summary>
    /// Gets or sets the total duration of this phase in milliseconds. Set on completion.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public long? DurationMs { get; set; }

    /// <summary>
    /// Gets or sets the live performance statistics for this phase.
    /// Populated at read-time for running phases only; not persisted.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public PhaseStats? Stats { get; set; }
}
