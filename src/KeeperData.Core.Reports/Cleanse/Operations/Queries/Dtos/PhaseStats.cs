using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;

/// <summary>
/// Per-phase live performance statistics including RPM and projected completion.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO class - no logic to test.")]
public class PhaseStats
{
    /// <summary>
    /// Gets or sets the current records per minute rate based on the latest sliding window.
    /// </summary>
    public double CurrentRpm { get; set; }

    /// <summary>
    /// Gets or sets the average records per minute over the entire phase lifetime.
    /// </summary>
    public double AverageRpm { get; set; }

    /// <summary>
    /// Gets or sets the projected UTC end time for this phase based on the current window RPM.
    /// Null if projection is not possible.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? ProjectedEndUtc { get; set; }

    /// <summary>
    /// Gets or sets the estimated remaining duration in seconds for this phase.
    /// Null if estimation is not possible.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public double? EstimatedRemainingSeconds { get; set; }

    /// <summary>
    /// Gets or sets the name of the currently active throttle policy.
    /// </summary>
    public string ThrottlePolicyName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the slug of the currently active throttle policy.
    /// </summary>
    public string ThrottlePolicySlug { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the current batch size from the active throttle policy.
    /// </summary>
    public int BatchSize { get; set; }

    /// <summary>
    /// Gets or sets the current delay in milliseconds between batches.
    /// </summary>
    public int BatchDelayMs { get; set; }
}
