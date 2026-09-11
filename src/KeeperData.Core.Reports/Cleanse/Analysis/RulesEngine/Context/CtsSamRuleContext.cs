using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;

/// <summary>
/// Everything a cleanse rule is permitted to see for a single record under evaluation.
/// </summary>
/// <remarks>
/// Built once per record by the engine and handed to every rule for that pass. Rules never
/// reach back into repositories or services, which keeps them pure and directly unit testable.
/// Derived collections such as <see cref="CtsEmails"/> are computed once and cached, so that
/// several rules inspecting the same data do not repeat the work.
/// </remarks>
public sealed class CtsSamRuleContext
{
    private string[]? _ctsEmails;
    private string[]? _samEmails;
    private string[]? _ctsPhones;
    private string[]? _samPhones;
    private string[]? _emailsMissingFromSam;
    private string[]? _phonesMissingFromSam;

    /// <summary>
    /// Gets the pass this context was built for. Only rules declaring the same pass are evaluated.
    /// </summary>
    public required AnalysisPass Pass { get; init; }

    /// <summary>
    /// Gets the CPH under evaluation. Always present, on both passes.
    /// </summary>
    public required Cph Cph { get; init; }

    /// <summary>
    /// Gets the CTS LID full identifier. Present on the CTS primary pass only.
    /// </summary>
    public LidFullIdentifier? LidFullIdentifier { get; init; }

    /// <summary>
    /// Gets the CTS holding for this record, or null when no CTS holding exists.
    /// </summary>
    public CtsCphHoldingModel? Cts { get; init; }

    /// <summary>
    /// Gets the SAM holding for this record, or null when no SAM holding exists.
    /// </summary>
    public SamCphHoldingModel? Sam { get; init; }

    /// <summary>
    /// Gets a value indicating whether a CTS holding was found.
    /// </summary>
    public bool ExistsInCts => Cts is not null;

    /// <summary>
    /// Gets a value indicating whether a SAM holding was found.
    /// </summary>
    public bool ExistsInSam => Sam is not null;

    /// <summary>
    /// Gets a value indicating whether the record was found in both systems.
    /// Comparison rules use this as their precondition.
    /// </summary>
    public bool ExistsInBoth => ExistsInCts && ExistsInSam;

    /// <summary>
    /// Gets the distinct email addresses held in CTS, or an empty array when there is no CTS holding.
    /// </summary>
    public string[] CtsEmails => _ctsEmails ??= Cts?.GetEmailAddresses() ?? [];

    /// <summary>
    /// Gets the distinct email addresses held in SAM, or an empty array when there is no SAM holding.
    /// </summary>
    public string[] SamEmails => _samEmails ??= Sam?.GetEmailAddresses() ?? [];

    /// <summary>
    /// Gets the distinct phone numbers held in CTS, or an empty array when there is no CTS holding.
    /// </summary>
    public string[] CtsPhones => _ctsPhones ??= Cts?.GetPhoneNumbers() ?? [];

    /// <summary>
    /// Gets the distinct phone numbers held in SAM, or an empty array when there is no SAM holding.
    /// </summary>
    public string[] SamPhones => _samPhones ??= Sam?.GetPhoneNumbers() ?? [];

    /// <summary>
    /// Gets the CTS email addresses that are absent from SAM.
    /// </summary>
    public string[] EmailsMissingFromSam => _emailsMissingFromSam ??= [.. CtsEmails.Except(SamEmails)];

    /// <summary>
    /// Gets the CTS phone numbers that are absent from SAM.
    /// </summary>
    public string[] PhonesMissingFromSam => _phonesMissingFromSam ??= [.. CtsPhones.Except(SamPhones)];

    /// <summary>
    /// Builds a rule result carrying the identity fields for this record.
    /// </summary>
    /// <param name="descriptor">The descriptor of the rule raising the issue.</param>
    /// <param name="contextProvider">Optional callback to populate rule specific issue context.</param>
    public RuleResult Issue(RuleDescriptor descriptor, Action<IssueContextData>? contextProvider = null)
    {
        var issueContext = new IssueContextData
        {
            SamCph = Cph.Value,
            CtsLidFullIdentifier = LidFullIdentifier?.Value
        };

        contextProvider?.Invoke(issueContext);

        return RuleResult.Issue(descriptor, issueContext);
    }
}
