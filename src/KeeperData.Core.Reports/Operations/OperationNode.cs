using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace KeeperData.Core.Reports.Operations;

/// <summary>
/// Immutable snapshot of a single node in the operation tree.
/// Combines timing, progress, and rate metrics into one self-describing structure.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO class - no logic to test.")]
public sealed class OperationNode
{
    public string Name { get; init; } = string.Empty;

    public string Status { get; init; } = OperationStatuses.NotStarted;

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Description { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public double? PercentComplete { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? ProcessedCount { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? TotalRecords { get; init; }

    public long ElapsedMs { get; init; }

    public string Elapsed { get; init; } = FormatElapsed(0);

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public long? ProjectedRemainingMs { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? ProjectedEndTimeUtc { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public double? CurrentRecordsPerMinute { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public double? AverageRecordsPerMinute { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public List<OperationNode>? Children { get; init; }

    /// <summary>
    /// Formats a millisecond value as "hh:mm:ss.f".
    /// </summary>
    public static string FormatElapsed(long ms)
    {
        var ts = TimeSpan.FromMilliseconds(ms);
        return ts.ToString(@"hh\:mm\:ss\.f");
    }
}
