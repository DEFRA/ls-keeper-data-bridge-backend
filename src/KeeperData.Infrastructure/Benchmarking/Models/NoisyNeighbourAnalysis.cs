namespace KeeperData.Infrastructure.Benchmarking.Models;

/// <summary>
/// Diagnostic analysis of a benchmark report for noisy-neighbour indicators.
/// Each flag represents a symptom of shared-resource contention that would
/// NOT be present against a healthy, uncontested MongoDB instance.
/// </summary>
public sealed record NoisyNeighbourAnalysis
{
    /// <summary>Overall verdict: true if any red flag fired.</summary>
    public bool HasRedFlags => Flags.Count > 0;

    /// <summary>
    /// Overall risk assessment computed from the highest-severity flag.
    /// "None" when no flags are present.
    /// </summary>
    public RiskLevel OverallRisk => Flags.Count == 0
        ? RiskLevel.None
        : Flags.Max(f => f.Severity);

    /// <summary>
    /// Plain-language summary of the most likely root cause, or null when healthy.
    /// Cross-correlates multiple flags to form a diagnosis rather than a symptom list.
    /// </summary>
    public string? ProbableCause { get; init; }

    /// <summary>Individual red-flag diagnostics with human-readable explanations.</summary>
    public IReadOnlyList<RedFlag> Flags { get; init; } = Array.Empty<RedFlag>();
}
