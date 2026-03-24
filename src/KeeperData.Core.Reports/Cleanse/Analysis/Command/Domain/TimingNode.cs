using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

/// <summary>
/// A node in the hierarchical timing tree. Each node tracks a named operation
/// with its cumulative elapsed time and optional children for sub-operations.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO class - no logic to test.")]
public sealed class TimingNode
{
    /// <summary>
    /// Gets or sets the name of this timing segment (e.g., "CTS Pump", "fetching").
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the cumulative elapsed time in milliseconds.
    /// For leaf nodes this is the directly tracked time; for parent nodes
    /// it is the sum of all children.
    /// </summary>
    public long ElapsedMs { get; set; }

    /// <summary>
    /// Gets or sets the human-readable formatted elapsed time (e.g., "00:05:12.3").
    /// </summary>
    public string Elapsed { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the child timing nodes, if any.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public List<TimingNode>? Children { get; set; }

    /// <summary>
    /// Formats a millisecond value as "hh:mm:ss.f".
    /// </summary>
    public static string FormatElapsed(long ms)
    {
        var ts = TimeSpan.FromMilliseconds(ms);
        return ts.ToString(@"hh\:mm\:ss\.f");
    }
}
