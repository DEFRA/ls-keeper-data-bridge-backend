using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Issues.Command.Requests;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Impl;

public class CleanseAnalysisEngine(ICtsSamQueryService dataService, IIssueCommandService issueCommandService) 
    : CleanseAnalysisEngineBase(dataService, issueCommandService), ICleanseAnalysisEngine
{
    private readonly RecordIdGenerator _recordIdGenerator = new();
    private readonly ICtsSamQueryService _dataService = dataService;

    private async Task ProcessCtsPrimaryRecordInternalAsync(LidFullIdentifier lidFullIdentifier, string operationId, AnalysisMetrics metrics, CancellationToken ct)
    {
        var samCphHolding = await _dataService.GetSamCphHoldingAsync(lidFullIdentifier.Cph, ct);
        var results = new List<RuleResult>();

        // PRIORITY 1A: RULE 2A - CPH present in CTS but missing in SAM (1B done in `ProcessSamPrimaryRecordInternalAsync`)
        if (samCphHolding is null)
        {
            results.Add(RuleResult.Issue(RuleDescriptors.CtsCphNotInSam, lidFullIdentifier));
            await RecordResultsAsync(lidFullIdentifier.Value, lidFullIdentifier.Cph, operationId, metrics, results, ct);
            return;
        }

        var ctsHolding = await _dataService.GetCtsCphHoldingAsync(lidFullIdentifier, ct);
        if (ctsHolding != null)
        {
            EvaluateCtsSamRules(ctsHolding, samCphHolding, results);
        }

        await RecordResultsAsync(lidFullIdentifier.Value, lidFullIdentifier.Cph, operationId, metrics, results, ct);
    }

    private static void EvaluateCtsSamRules(CtsCphHoldingModel ctsHolding, SamCphHoldingModel samCphHolding, List<RuleResult> results)
    {
        var ctsEmails = ctsHolding.GetEmailAddresses();
        var samEmails = samCphHolding.GetEmailAddresses();
        var ctsPhones = ctsHolding.GetPhoneNumbers();
        var samPhones = samCphHolding.GetPhoneNumbers();

        // PRIORITY 2:  RULE 4: CPH present in both CTS and SAM but no email addresses in either system
        if (ctsEmails.Length + samEmails.Length == 0) 
        {
            results.Add(RuleResult.Issue(RuleDescriptors.CtsSamNoEmailAddresses, ctsHolding.Id, samCphHolding.Cph));
        }

        // PRIORITY 3:  RULE 12 - Email addresses in CTS missing from SAM
        var missingEmails = ctsEmails.Except(samEmails).ToArray();
        if (missingEmails.Length > 0)
        {
            results.Add(RuleResult.Issue(RuleDescriptors.SamMissingEmailAddresses, ctsHolding.Id, samCphHolding.Cph, x => x.EmailCTS = missingEmails));
        }

        // PRIORITY 4:  RULE 5  - CPH present in both CTS and SAM but no phone numbers in either system
        if (ctsPhones.Length + samPhones.Length == 0)
        {
            results.Add(RuleResult.Issue(RuleDescriptors.CtsSamNoPhoneNumbers, ctsHolding.Id, samCphHolding.Cph));
        }

        // PRIORITY 5:  RULE 11 - CTS phone numbers missing from SAM
        var missingPhones = ctsPhones.Except(samPhones).ToArray();
        if (missingPhones.Length > 0)
        {
            results.Add(RuleResult.Issue(RuleDescriptors.SamMissingPhoneNumbers, ctsHolding.Id, samCphHolding.Cph, x => x.TelCTS = missingPhones));
        }

        // PRIORITY 6:  RULE 1  - No cattle unit defined in SAM
        var asc = samCphHolding.Holding[DataFields.SamCphHoldingFields.AnimalSpeciesCode]?.ToString();
        if (asc != "CTT")
        {
            results.Add(RuleResult.Issue(RuleDescriptors.SamNoCattleUnit, ctsHolding.Id, samCphHolding.Cph, x => x.AnimalSpeciesCode = asc));
        }

        // PRIORITY 10: RULE 3 - Cattle-related CPHs in SAM (e.g. those with relevant animal species or purpose codes) that are not present in CTS
        // aka: where ANIMAL_SPECIES_CODE=CTT - if SAM.FEATURE_NAME=['Unknown','Not known','Notknown','',null] OR CTS.ADR_NAME != SAM.FEATURE_NAME then raise issue
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
        var results = new List<RuleResult>();

        // get the CTS CPH Holding...
        var ctsCphHolding = await _dataService.GetCtsCphHoldingAsync(cph, ct);
        if (ctsCphHolding is null) // does not exist
        {
            results.Add(RuleResult.Issue(RuleDescriptors.SamCphNotInCts, cph)); // PRIORITY 1B: RULE 2B - CPH present in SAM but missing in CTS
        }

        await RecordResultsAsync(cph.Value, cph, operationId, metrics, results, ct);
    }

    #region Overrides 

    protected override async Task ProcessCtsPrimaryRecordAsync(string id, string operationId, AnalysisMetrics metrics, CancellationToken ct)
    {
        var lidFullIdentifier = LidFullIdentifier.TryParse(id);

        if (lidFullIdentifier is not null && IsValidCountyCode(lidFullIdentifier))
        {
            await ProcessCtsPrimaryRecordInternalAsync(lidFullIdentifier, operationId, metrics, ct);
        }
    }

    protected override async Task ProcessSamPrimaryRecordAsync(string id, string operationId, AnalysisMetrics metrics, CancellationToken ct)
    {
        var cph = Cph.TryParse(id);

        if (cph is not null)
        {
            await ProcessSamPrimaryRecordInternalAsync(cph, operationId, metrics, ct);
        }
    }

    #endregion

    #region Helpers

    private async Task RecordResultsAsync(string primaryRecordId, Cph cph, string operationId,
        AnalysisMetrics metrics, List<RuleResult> results, CancellationToken ct)
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

            var recordResult = await IssueCommandService.RecordIssueAsync(command, ct);

            if (recordResult is IssueRecordResult.Created or IssueRecordResult.Reactivated)
            {
                metrics.IssuesFound++;
            }
        }
    }

    /// <summary>
    /// County Code must be between 1 and 51 (inclusive) to be valid
    /// </summary>
    protected static bool IsValidCountyCode(LidFullIdentifier lidFullIdentifier)
        => lidFullIdentifier.Cph.CountyCode.ToInteger() is >= 1 and <= 51;

    protected string GenerateThumbprint(string primaryRecordId, string ruleId)
        => _recordIdGenerator.GenerateId($"{primaryRecordId}:{ruleId}");

    #endregion
}


