using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Analysis;
using KeeperData.Core.Reports.Rules;

namespace KeeperData.Core.Reports.Strategies.Rules;

/// <summary>
/// Rule that compares CTS CPH Holding records with SAM CPH Holding records
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Rule with database query dependency - covered by integration tests.")]
public sealed class SamCphRecordNoEmailAddressesRule : BaseSamCphPartyComparisonRule, ICleanseRule<CtsSamRuleInput>
{
    /// <inheritdoc />
    public string RuleCode => "CTS_SAM_NO_EMAIL_ADDRESSES";

    protected override Task<RuleResult> ExecuteAsync(CtsSamRuleInput input, IAnalysisContext context, CancellationToken ct, 
        QueryResult ctsKeepers, QueryResult samParties)
    {
        var ctsEmailAddresses = ctsKeepers.Data
            .Select(x => x[DataFields.CtsKeeperFields.ParEmailAddress]?.ToString())
            .Where(s => s != null && !string.IsNullOrWhiteSpace(s))
            .Distinct()
            .ToList();

        var samEmailAddresses = samParties.Data
            .Select(x => x[DataFields.SamPartyFields.InternetEmailAddress]?.ToString())
            .Where(s => s != null && !string.IsNullOrWhiteSpace(s))
            .Distinct()
            .ToList();

        var emailAddresses = samEmailAddresses.Concat(ctsEmailAddresses).Distinct().ToList();

        if (emailAddresses.Count == 0)
        {
            return Task.FromResult(RuleResult.Issue(RuleCode));
        }

        return Task.FromResult(RuleResult.NoIssue());
    }

}
