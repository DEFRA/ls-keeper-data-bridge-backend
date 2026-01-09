using KeeperData.Core.Reports.Analysis;
using KeeperData.Core.Reports.Rules;

namespace KeeperData.Core.Reports.Strategies.Rules;

/// <summary>
/// Rule that checks if a SAM CPH Holding record exists for the CTS CPH.
/// If no SAM record is found, an issue is recorded.
/// </summary>
public sealed class SamCphRecordDoesNotExistRule : ICleanseRule<CtsSamRuleInput>
{
    /// <inheritdoc />
    public string RuleCode => IssueCodes.CTS_CPH_NOT_IN_SAM;

    /// <inheritdoc />
    public async Task<RuleResult> ExecuteAsync(
        CtsSamRuleInput input,
        IAnalysisContext context,
        CancellationToken ct)
    {
        var samRecord = await context.QuerySingleAsync(QueryParametersFactory.GetSamCphHolding(context.DataSets, 
            input.LidFullIdentifier.Cph), ct);

        if (samRecord is null)
        {
            return RuleResult.Issue(
                IssueCodes.CTS_CPH_NOT_IN_SAM,
                new Dictionary<string, object?>
                {
                    ["Cph"] = input.LidFullIdentifier.Cph.Value,
                    ["LidFullIdentifier"] = input.LidFullIdentifier.Value
                });
        }

        input.SamCphHoldingRecord = samRecord;

        return RuleResult.NoIssue();
    }
}
