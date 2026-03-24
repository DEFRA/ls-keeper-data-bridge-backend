using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Abstract;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Issues.Command.Requests;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using KeeperData.Core.Throttling;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Impl;

public class CleanseAnalysisEngine(IPreloadedCtsSamDataService dataService, IIssueCommandService issueCommandService,
    IThrottler throttler, ICleanseRunStatsService runStatsService, ILogger<CleanseAnalysisEngine> logger)
    : CleanseAnalysisEngineBase(dataService, issueCommandService, throttler, runStatsService, logger), ICleanseAnalysisEngine
{
    private readonly RecordIdGenerator _recordIdGenerator = new();
    private readonly IPreloadedCtsSamDataService _dataService = dataService;

    private async Task ProcessCtsPrimaryRecordInternalAsync(LidFullIdentifier lidFullIdentifier, string operationId, AnalysisMetrics metrics, CancellationToken ct)
    {
        Trace.TraceInformation($"KRDSBRIDGE | ProcessCtsPrimaryRecordInternal | BEGIN, lid={lidFullIdentifier.Value}, operationId={operationId}");
        var sw = Stopwatch.StartNew();

        var samCphHolding = _dataService.GetSamCphHolding(lidFullIdentifier.Cph);
        var ctsHolding = _dataService.GetCtsCphHolding(lidFullIdentifier);
        Trace.TraceInformation($"KRDSBRIDGE | ProcessCtsPrimaryRecordInternal | Lookups done, lid={lidFullIdentifier.Value}, samFound={samCphHolding is not null}, ctsFound={ctsHolding is not null}, elapsed={sw.ElapsedMilliseconds}ms");
        var results = new List<RuleResult>();

        // PRIORITY 1A: RULE 2A - CPH present in CTS but missing in SAM (1B done in `ProcessSamPrimaryRecordInternalAsync`)
        if (samCphHolding is null)
        {
            Trace.TraceInformation($"KRDSBRIDGE | ProcessCtsPrimaryRecordInternal | SAM CPH not found for lid={lidFullIdentifier.Value}");
            results.Add(RuleResult.Issue(RuleDescriptors.CtsCphNotInSam, lidFullIdentifier));
            await RecordResultsAsync(lidFullIdentifier.Value, lidFullIdentifier.Cph, operationId, metrics, results, ct);
            sw.Stop();
            Trace.TraceInformation($"KRDSBRIDGE | ProcessCtsPrimaryRecordInternal | END (missing SAM), lid={lidFullIdentifier.Value}, issues={results.Count}, duration={sw.ElapsedMilliseconds}ms");
            return;
        }

        if (ctsHolding != null)
        {
            EvaluateCtsSamRules(ctsHolding, samCphHolding, results);
        }

        await RecordResultsAsync(lidFullIdentifier.Value, lidFullIdentifier.Cph, operationId, metrics, results, ct);
        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | ProcessCtsPrimaryRecordInternal | END, lid={lidFullIdentifier.Value}, issues={results.Count}, duration={sw.ElapsedMilliseconds}ms");
    }

    private static void EvaluateCtsSamRules(CtsCphHoldingModel ctsHolding, SamCphHoldingModel samCphHolding, List<RuleResult> results)
    {
        EvaluateEmailRules(ctsHolding, samCphHolding, results);
        EvaluatePhoneRules(ctsHolding, samCphHolding, results);
        EvaluateCattleUnitRule(ctsHolding, samCphHolding, results);
        EvaluateLocationConsistencyRule(ctsHolding, samCphHolding, results);
    }

    private static void EvaluateEmailRules(CtsCphHoldingModel ctsHolding, SamCphHoldingModel samCphHolding, List<RuleResult> results)
    {
        var ctsEmails = ctsHolding.GetEmailAddresses();
        var samEmails = samCphHolding.GetEmailAddresses();

        // PRIORITY 2: RULE 4 - CPH present in both CTS and SAM but no email addresses in either system
        if (ctsEmails.Length + samEmails.Length == 0)
        {
            results.Add(RuleResult.Issue(RuleDescriptors.CtsSamNoEmailAddresses, ctsHolding.Id, samCphHolding.Cph));
        }

        var missingEmails = ctsEmails.Except(samEmails).ToArray();
        if (missingEmails.Length > 0)
        {
            if (samEmails.Length == 0) // PRIORITY 3: RULE 12 - Email addresses in CTS missing from SAM
            {
                results.Add(RuleResult.Issue(RuleDescriptors.SamMissingEmailAddresses, ctsHolding.Id, samCphHolding.Cph,
                    x => x.EmailCTS = missingEmails));
            }
            else // PRIORITY 7: RULE 6 - Email addresses inconsistent between CTS and SAM
            {
                results.Add(RuleResult.Issue(RuleDescriptors.CtsSamEmailAddressesInconsistent, ctsHolding.Id, samCphHolding.Cph, x =>
                            {
                                x.EmailCTS = missingEmails;
                                x.EmailSAM = string.Join("; ", samEmails);
                            }));
            }
        }
    }

    private static void EvaluatePhoneRules(CtsCphHoldingModel ctsHolding, SamCphHoldingModel samCphHolding, List<RuleResult> results)
    {
        var ctsPhones = ctsHolding.GetPhoneNumbers();
        var samPhones = samCphHolding.GetPhoneNumbers();

        // PRIORITY 4: RULE 5 - CPH present in both CTS and SAM but no phone numbers in either system
        if (ctsPhones.Length + samPhones.Length == 0)
        {
            results.Add(RuleResult.Issue(RuleDescriptors.CtsSamNoPhoneNumbers, ctsHolding.Id, samCphHolding.Cph));
        }

        var missingPhones = ctsPhones.Except(samPhones).ToArray();
        if (missingPhones.Length > 0)
        {
            if (samPhones.Length == 0) // PRIORITY 5: RULE 11 - CTS phone numbers missing from SAM
            {
                results.Add(RuleResult.Issue(RuleDescriptors.SamMissingPhoneNumbers, ctsHolding.Id, samCphHolding.Cph,
                    x => x.TelCTS = missingPhones));
            }
            else
            {
                // PRIORITY 8: RULE 7 - Phone numbers inconsistent between CTS and SAM
                results.Add(RuleResult.Issue(RuleDescriptors.CtsSamPhoneNosInconsistent, ctsHolding.Id, samCphHolding.Cph, x =>
                {
                    x.TelCTS = missingPhones;
                    x.TelSAM = string.Join("; ", samPhones);
                }));
            }
        }
    }

    private static void EvaluateCattleUnitRule(CtsCphHoldingModel ctsHolding, SamCphHoldingModel samCphHolding, List<RuleResult> results)
    {
        // PRIORITY 6: RULE 1 - No cattle unit defined in SAM
        var asc = samCphHolding.Holding[DataFields.SamCphHoldingFields.AnimalSpeciesCode]?.ToString();
        if (asc != "CTT")
        {
            results.Add(RuleResult.Issue(RuleDescriptors.SamNoCattleUnit, ctsHolding.Id, samCphHolding.Cph,
                x => x.AnimalSpeciesCode = asc));
        }
    }

    private static void EvaluateLocationConsistencyRule(CtsCphHoldingModel ctsHolding, SamCphHoldingModel samCphHolding, List<RuleResult> results)
    {
        // PRIORITY 10: RULE 3 - Cattle-related CPHs in SAM with mismatched or unknown location names
        if (samCphHolding.AnimalSpeciesCode == "CTT" && IsLocationMismatch(ctsHolding, samCphHolding))
        {
            results.Add(RuleResult.Issue(RuleDescriptors.CtsSamLocationsDiffer, ctsHolding.Id, samCphHolding.Cph,
                x =>
                {
                    x.LocationNameSAM = samCphHolding.LocationName;
                    x.LocationNameCTS = ctsHolding.LocationName;
                }));
        }
    }

    private static bool IsLocationMismatch(CtsCphHoldingModel ctsHolding, SamCphHoldingModel samCphHolding)
    {
        return string.IsNullOrWhiteSpace(samCphHolding.LocationName)
            || samCphHolding.LocationName.Equals("unknown", StringComparison.OrdinalIgnoreCase)
            || samCphHolding.LocationName.Equals("not known", StringComparison.OrdinalIgnoreCase)
            || samCphHolding.LocationName.Equals("notknown", StringComparison.OrdinalIgnoreCase)
            || !string.Equals(ctsHolding.LocationName, samCphHolding.LocationName, StringComparison.OrdinalIgnoreCase);
    }

    private async Task ProcessSamPrimaryRecordInternalAsync(Cph cph, string operationId, AnalysisMetrics metrics, CancellationToken ct)
    {
        Trace.TraceInformation($"KRDSBRIDGE | ProcessSamPrimaryRecordInternal | BEGIN, cph={cph.Value}, operationId={operationId}");
        var sw = Stopwatch.StartNew();
        var results = new List<RuleResult>();

        var ctsCphHolding = _dataService.GetCtsCphHolding(cph);

        if (ctsCphHolding is null) // does not exist
        {
            Trace.TraceInformation($"KRDSBRIDGE | ProcessSamPrimaryRecordInternal | CTS CPH not found for cph={cph.Value}");
            results.Add(RuleResult.Issue(RuleDescriptors.SamCphNotInCts, cph)); // PRIORITY 1B: RULE 2B - CPH present in SAM but missing in CTS
        }

        await RecordResultsAsync(cph.Value, cph, operationId, metrics, results, ct);
        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | ProcessSamPrimaryRecordInternal | END, cph={cph.Value}, issues={results.Count}, duration={sw.ElapsedMilliseconds}ms");
    }

    protected override async Task ProcessCtsPrimaryRecordAsync(string id, string operationId, AnalysisMetrics metrics, CancellationToken ct)
    {
        var lidFullIdentifier = LidFullIdentifier.TryParse(id);

        if (lidFullIdentifier is not null && IsValidCountyCode(lidFullIdentifier))
        {
            await ProcessCtsPrimaryRecordInternalAsync(lidFullIdentifier, operationId, metrics, ct);
        }
        else
        {
            Trace.TraceInformation($"KRDSBRIDGE | ProcessCtsPrimaryRecordAsync | Skipped invalid record, id={id}");
        }
    }

    protected override async Task ProcessSamPrimaryRecordAsync(string id, string operationId, AnalysisMetrics metrics, CancellationToken ct)
    {
        var cph = Cph.TryParse(id);

        if (cph is not null && IsValidCountyCode(cph))
        {
            await ProcessSamPrimaryRecordInternalAsync(cph, operationId, metrics, ct);
        }
        else
        {
            Trace.TraceInformation($"KRDSBRIDGE | ProcessSamPrimaryRecordAsync | Skipped invalid record, id={id}");
        }
    }

    private async Task RecordResultsAsync(string primaryRecordId, Cph cph, string operationId,
        AnalysisMetrics metrics, List<RuleResult> results, CancellationToken ct)
    {
        Trace.TraceInformation($"KRDSBRIDGE | RecordResultsAsync | BEGIN, primaryRecordId={primaryRecordId}, cph={cph.Value}, resultsCount={results.Count}");
        var rsw = Stopwatch.StartNew();
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

            var recordResult = await IssueCommandService.RecordIssueAsync(command, ct);

            if (recordResult is IssueRecordResult.Created or IssueRecordResult.Reactivated)
            {
                metrics.IssuesFound++;
            }

            await Throttler.DelayAsync(Throttler.Settings.CleanseAnalysis.RecordIssueDelayMs, ct);
        }
        rsw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | RecordResultsAsync | END, primaryRecordId={primaryRecordId}, resultsCount={results.Count}, duration={rsw.ElapsedMilliseconds}ms");
    }

    /// <summary>
    /// County Code must be between 1 and 51 (inclusive) to be valid
    /// </summary>
    protected static bool IsValidCountyCode(LidFullIdentifier lidFullIdentifier)
        => IsValidCountyCode(lidFullIdentifier.Cph);

    /// <summary>
    /// County Code must be between 1 and 51 (inclusive) to be valid
    /// </summary>
    protected static bool IsValidCountyCode(Cph cph)
        => cph.CountyCode.ToInteger() is >= 1 and <= 51;

    protected string GenerateThumbprint(string primaryRecordId, string ruleId)
        => _recordIdGenerator.GenerateId($"{primaryRecordId}:{ruleId}");
}


