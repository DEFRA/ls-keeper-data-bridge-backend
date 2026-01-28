using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Strategies;

/// <summary>
/// Input data for rules in the CTS-SAM analysis strategy.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Simple data transfer object used only by analysis rules.")]
public sealed class CtsSamRuleInput
{
    /// <summary>
    /// Gets the CTS CPH Holding record being analyzed.
    /// </summary>
    public required IDictionary<string, object?> CtsRecord { get; init; }

    /// <summary>
    /// Gets the parsed LID full identifier from the CTS record.
    /// </summary>
    public required LidFullIdentifier LidFullIdentifier { get; init; }

    /// <summary>
    /// Gets the thumbprint (hash) for issue identification.
    /// </summary>
    public required string Thumbprint { get; init; }

    /// <summary>
    /// Gets or sets the corresponding SAM CPH Holding record, if found.
    /// Populated by rules that look up SAM data.
    /// </summary>
    public Dictionary<string, object?>? SamCphHoldingRecord { get; set; }

    /// <summary>
    /// Gets or sets the SAM CPH Holder record, if found.
    /// Populated by rules that look up holder data.
    /// </summary>
    public Dictionary<string, object?>? SamCphHolderRecord { get; set; }
}
