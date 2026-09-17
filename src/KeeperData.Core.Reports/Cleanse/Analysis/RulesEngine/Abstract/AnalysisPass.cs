namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;

/// <summary>
/// Identifies which iteration pass of the cleanse analysis a rule participates in.
/// </summary>
/// <remarks>
/// The analysis walks CTS holdings first, then SAM holdings. A rule declares its pass
/// explicitly so that pass membership is discoverable from the rule itself, rather than
/// being implied by which method happens to call it.
/// </remarks>
public enum AnalysisPass
{
    /// <summary>
    /// The rule is evaluated once per CTS CPH holding, with the matching SAM holding looked up.
    /// </summary>
    CtsPrimary,

    /// <summary>
    /// The rule is evaluated once per SAM CPH holding, with the matching CTS holding looked up.
    /// </summary>
    SamPrimary
}
