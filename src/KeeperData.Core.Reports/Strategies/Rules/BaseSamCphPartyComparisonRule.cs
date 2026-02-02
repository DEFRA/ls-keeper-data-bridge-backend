using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Analysis;
using KeeperData.Core.Reports.Rules;

namespace KeeperData.Core.Reports.Strategies.Rules;

/// <summary>
/// Rule that compares CTS CPH Holding records with SAM CPH Holding records
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Base rule class with database query dependency - covered by integration tests.")]
public abstract class BaseSamCphPartyComparisonRule
{
    protected abstract Task<RuleResult> ExecuteAsync(CtsSamRuleInput input, IAnalysisContext context, CancellationToken ct, 
        QueryResult ctsKeepers, QueryResult samParties);
    
    /// <inheritdoc />
    public async Task<RuleResult> ExecuteAsync(CtsSamRuleInput input, IAnalysisContext context, CancellationToken ct)
    {
        // This rule requires the SAM CPH Holding record to be populated by previous rules
        if (input.SamCphHoldingRecord is null)
        {
            return RuleResult.NoIssue();
        }

        // Get CTS Keeper records
        var ctsKeepersQuery  = QueryParametersFactory.GetCtsKeepers(context.DataSets, input.LidFullIdentifier);
        var ctsKeepers = await context.QueryAsync(ctsKeepersQuery, ct);

        // Get SAM Herd records
        var samHerdsQuery = QueryParametersFactory.GetSamHerdQuery(context.DataSets, input.LidFullIdentifier.Cph);
        var samHerds = await context.QueryAsync(samHerdsQuery, ct);

        var partyIds = samHerds.Data
            .SelectMany(x => new[]
            {
                x[DataFields.SamHerd.OwnerPartyIds]?.ToString(),
                x[DataFields.SamHerd.KeeperPartyIds]?.ToString()
            })
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .SelectMany(s => s!.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            .Distinct()
            .ToList();

        var samPartyQueries = partyIds.Select(partyId => QueryParametersFactory.GetSamPartiesQuery(context.DataSets, partyId)).ToList(); 
        var samPartiesResults = await Task.WhenAll(samPartyQueries.Select(q => context.QueryAsync(q, ct)));
        var samPartiesResults2 = QueryResult.Combine(samPartiesResults); // combine the sam parties result into one

        return await ExecuteAsync(input, context, ct, ctsKeepers, samPartiesResults2);
    }
}
