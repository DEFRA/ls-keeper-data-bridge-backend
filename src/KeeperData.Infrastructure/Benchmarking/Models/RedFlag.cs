namespace KeeperData.Infrastructure.Benchmarking.Models;

/// <summary>
/// A single noisy-neighbour diagnostic flag.
/// </summary>
public sealed record RedFlag
{
    /// <summary>Machine-readable category for comparison tooling.</summary>
    public string Category { get; init; } = default!;

    /// <summary>How critical this indicator is.</summary>
    public RiskLevel Severity { get; init; } = RiskLevel.Warning;

    /// <summary>Human-readable description of the problem observed.</summary>
    public string Description { get; init; } = default!;

    /// <summary>Actionable guidance on what to investigate next.</summary>
    public string Remediation { get; init; } = default!;

    /// <summary>The observed value that triggered the flag.</summary>
    public double ObservedValue { get; init; }

    /// <summary>The threshold that was exceeded.</summary>
    public double Threshold { get; init; }
}
