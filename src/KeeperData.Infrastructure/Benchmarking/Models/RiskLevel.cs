namespace KeeperData.Infrastructure.Benchmarking.Models;

/// <summary>
/// Severity / risk level for a noisy-neighbour diagnostic flag.
/// </summary>
public enum RiskLevel
{
    /// <summary>No issues detected.</summary>
    None = 0,

    /// <summary>Marginal — values are elevated but not yet critical.</summary>
    Warning = 1,

    /// <summary>Likely noisy-neighbour impact — investigate immediately.</summary>
    Critical = 2
}
