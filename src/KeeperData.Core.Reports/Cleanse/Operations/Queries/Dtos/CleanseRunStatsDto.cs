using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;

/// <summary>
/// Live performance statistics for a running cleanse analysis operation.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO class - no logic to test.")]
public class CleanseRunStatsDto
{
    /// <summary>
    /// Gets or sets the current records per minute rate based on the latest sliding window.
    /// </summary>
    public double CurrentRpm { get; set; }

    /// <summary>
    /// Gets or sets the average records per minute over the entire process lifetime.
    /// </summary>
    public double AverageRpm { get; set; }

    /// <summary>
    /// Gets or sets the projected UTC end time based on the average RPM and remaining records.
    /// Null if projection is not possible (e.g. no records analyzed yet).
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? ProjectedEndUtc { get; set; }

    /// <summary>
    /// Gets or sets the name of the currently active throttle policy.
    /// </summary>
    public string ThrottlePolicyName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the slug of the currently active throttle policy.
    /// </summary>
    public string ThrottlePolicySlug { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the current pump batch size from the active throttle policy.
    /// </summary>
    public int PumpBatchSize { get; set; }

    /// <summary>
    /// Gets or sets the current pump delay in milliseconds from the active throttle policy.
    /// </summary>
    public int PumpDelayMs { get; set; }
}
