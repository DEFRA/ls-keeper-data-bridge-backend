using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Issues.Command.Requests;
using KeeperData.Core.Reports.Operations;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Throttling;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Impl;

/// <summary>
/// Cleanse analysis engine that delegates all detection logic to registered
/// <see cref="ICleanseRule"/> implementations.
/// </summary>
/// <remarks>
/// Parallel implementation. Not registered in dependency injection and not active.
/// It derives from <see cref="CleanseAnalysisEngineBase"/> so that preloading, pumping,
/// batching, progress scopes, throttling and issue recording are reused unchanged, and only
/// the per record processing is reimplemented.
/// <para>
/// The engine contains no rule logic. Adding a rule never requires editing this file.
/// </para>
/// <para>
/// Rule ordering and pass filtering are performed here for now. Extracting them into a
/// dedicated rule registry is a later stage of this work.
/// </para>
/// </remarks>
public class RuleBasedCleanseAnalysisEngine : CleanseAnalysisEngineBase, ICleanseAnalysisEngine
{
    private readonly RecordIdGenerator _recordIdGenerator = new();
    private readonly IPreloadedCtsSamDataService _dataService;
    private readonly IReadOnlyList<ICleanseRule> _ctsPrimaryRules;
    private readonly IReadOnlyList<ICleanseRule> _samPrimaryRules;

    public RuleBasedCleanseAnalysisEngine(IPreloadedCtsSamDataService dataService, IIssueCommandService issueCommandService,
        IThrottler throttler, ILogger<RuleBasedCleanseAnalysisEngine> logger, IEnumerable<ICleanseRule> rules)
        : base(dataService, issueCommandService, throttler, logger)
    {
        _dataService = dataService;

        var activeRules = rules
            .Where(rule => rule.Status == RuleStatus.Active)
            .OrderBy(rule => rule.Priority)
            .ThenBy(rule => rule.Descriptor.UserRuleNo, StringComparer.Ordinal)
            .ToList();

        _ctsPrimaryRules = [.. activeRules.Where(rule => rule.Pass == AnalysisPass.CtsPrimary)];
        _samPrimaryRules = [.. activeRules.Where(rule => rule.Pass == AnalysisPass.SamPrimary)];
    }

    /// <inheritdoc />
    protected override async Task ProcessCtsPrimaryRecordAsync(string id, string operationId,
        AnalysisMetrics metrics, CancellationToken ct, OperationScope? scope = null)
    {
        var lidFullIdentifier = LidFullIdentifier.TryParse(id);

        if (lidFullIdentifier is null || !IsValidCountyCode(lidFullIdentifier))
        {
            return;
        }

        var context = new CtsSamRuleContext
        {
            Pass = AnalysisPass.CtsPrimary,
            Cph = lidFullIdentifier.Cph,
            LidFullIdentifier = lidFullIdentifier,
            Cts = _dataService.GetCtsCphHolding(lidFullIdentifier),
            Sam = _dataService.GetSamCphHolding(lidFullIdentifier.Cph)
        };

        var results = EvaluateRules(_ctsPrimaryRules, context);

        await RecordResultsAsync(lidFullIdentifier.Value, lidFullIdentifier.Cph, operationId, metrics, results, ct, scope);
    }

    /// <inheritdoc />
    protected override async Task ProcessSamPrimaryRecordAsync(string id, string operationId,
        AnalysisMetrics metrics, CancellationToken ct, OperationScope? scope = null)
    {
        var cph = Cph.TryParse(id);

        if (cph is null || !IsValidCountyCode(cph))
        {
            return;
        }

        var context = new CtsSamRuleContext
        {
            Pass = AnalysisPass.SamPrimary,
            Cph = cph,
            Cts = _dataService.GetCtsCphHolding(cph),
            Sam = _dataService.GetSamCphHolding(cph)
        };

        var results = EvaluateRules(_samPrimaryRules, context);

        await RecordResultsAsync(cph.Value, cph, operationId, metrics, results, ct, scope);
    }

    /// <summary>
    /// Runs the supplied rules, in priority order, against one record context.
    /// </summary>
    private static List<RuleResult> EvaluateRules(IReadOnlyList<ICleanseRule> rules, CtsSamRuleContext context)
    {
        var results = new List<RuleResult>();

        foreach (var rule in rules)
        {
            if (!rule.AppliesTo(context))
            {
                continue;
            }

            var result = rule.Evaluate(context);

            if (result is not null)
            {
                results.Add(result);
            }
        }

        return results;
    }

    /// <summary>
    /// Persists the detected issues using the shared issue command service, applying the
    /// configured inter record throttle. Mirrors the recording behaviour of the existing engine.
    /// </summary>
    private async Task RecordResultsAsync(string primaryRecordId, Cph cph, string operationId,
        AnalysisMetrics metrics, List<RuleResult> results, CancellationToken ct, OperationScope? scope)
    {
        foreach (var result in results)
        {
            var thumbprint = GenerateThumbprint(primaryRecordId, result.Descriptor.RuleId);

            var command = new RecordIssueCommand(
                operationId,
                thumbprint,
                result.Descriptor,
                cph,
                result.IssueContext?.CtsLidFullIdentifier,
                result.IssueContext);

            var (recordResult, recordMs) = await Timed.RunAsync(() => IssueCommandService.RecordIssueAsync(command, ct));
            scope?.TrackElapsed("issue_recording", recordMs);

            if (recordResult is IssueRecordResult.Created or IssueRecordResult.Reactivated)
            {
                metrics.IssuesFound++;
            }

            var throttleMs = await Timed.RunAsync(() => Throttler.DelayAsync(Throttler.Settings.CleanseAnalysis.RecordIssueDelayMs, ct));
            scope?.TrackElapsed("throttle_wait", throttleMs);
        }
    }

    /// <summary>
    /// County Code must be between 1 and 51 (inclusive) to be valid.
    /// </summary>
    protected static bool IsValidCountyCode(LidFullIdentifier lidFullIdentifier)
        => IsValidCountyCode(lidFullIdentifier.Cph);

    /// <summary>
    /// County Code must be between 1 and 51 (inclusive) to be valid.
    /// </summary>
    protected static bool IsValidCountyCode(Cph cph)
        => cph.CountyCode.ToInteger() is >= 1 and <= 51;

    /// <summary>
    /// Generates the stable issue thumbprint for a record and rule pairing.
    /// </summary>
    protected string GenerateThumbprint(string primaryRecordId, string ruleId)
        => _recordIdGenerator.GenerateId($"{primaryRecordId}:{ruleId}");
}
