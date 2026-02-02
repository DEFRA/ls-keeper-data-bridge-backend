using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Analysis;
using KeeperData.Core.Reports.Rules;

namespace KeeperData.Core.Reports.Strategies.Rules;

/// <summary>
/// Rule that compares CTS CPH Holding records with SAM CPH Holding records
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Rule with database query dependency - covered by integration tests.")]
public sealed class SamCphRecordNoPhoneNumbersRule : BaseSamCphPartyComparisonRule, ICleanseRule<CtsSamRuleInput>
{
    /// <inheritdoc />
    public string RuleCode => "CTS_SAM_NO_PHONE_NUMBERS";

    protected override Task<RuleResult> ExecuteAsync(CtsSamRuleInput input, IAnalysisContext context, CancellationToken ct,
        QueryResult ctsKeepers, QueryResult samParties)
    {
        string[] ctsPhoneNumbers =
        [
            input.CtsRecord[DataFields.CtsCphHoldingFields.LocMobileNumber]?.ToString() ?? string.Empty,
            input.CtsRecord[DataFields.CtsCphHoldingFields.LocTelNumber]?.ToString() ?? string.Empty,
        ];

        var ctsKeeperPhoneNumbers = ctsKeepers.Data
            .SelectMany(x => new[]
            {
                x[DataFields.CtsKeeperFields.ParMobileNumber]?.ToString(),
                x[DataFields.CtsKeeperFields.ParTelNumber]?.ToString()
            })
            .Where(s => !string.IsNullOrWhiteSpace(s));

        var samPhoneNumbers = samParties.Data
            .SelectMany(x => new[]
            {
                x[DataFields.SamPartyFields.TelephoneNumber]?.ToString(),
                x[DataFields.SamPartyFields.MobileNumber]?.ToString()
            })
            .Where(s => !string.IsNullOrWhiteSpace(s));

        var hasAnyPhoneNumber = samPhoneNumbers
            .Concat(ctsKeeperPhoneNumbers)
            .Concat(ctsPhoneNumbers)
            .Any(s => !string.IsNullOrWhiteSpace(s));

        return Task.FromResult(hasAnyPhoneNumber ? RuleResult.NoIssue() : RuleResult.Issue(RuleCode));
    }

}
