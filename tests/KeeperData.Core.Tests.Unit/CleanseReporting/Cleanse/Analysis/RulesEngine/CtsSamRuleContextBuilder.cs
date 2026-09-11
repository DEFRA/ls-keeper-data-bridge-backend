using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.RulesEngine;

/// <summary>
/// Builds rule contexts for rule unit tests without touching the data service.
/// </summary>
internal static class CtsSamRuleContextBuilder
{
    public const string DefaultLid = "UK-12/345/6001";
    public const string DefaultCph = "12/345/6001";

    public static CtsSamRuleContext Build(
        AnalysisPass pass = AnalysisPass.CtsPrimary,
        bool includeCts = true,
        bool includeSam = true,
        string? samAnimalSpeciesCode = "CTT",
        string? samLocationName = "Green Farm",
        string? ctsLocationName = "Green Farm",
        string[]? ctsEmails = null,
        string[]? samEmails = null,
        string[]? ctsPhones = null,
        string[]? samPhones = null)
    {
        var lid = LidFullIdentifier.TryParse(DefaultLid)!;
        var cph = Cph.TryParse(DefaultCph)!;

        return new CtsSamRuleContext
        {
            Pass = pass,
            Cph = cph,
            LidFullIdentifier = pass == AnalysisPass.CtsPrimary ? lid : null,
            Cts = includeCts ? BuildCts(lid, ctsLocationName, ctsEmails ?? [], ctsPhones ?? []) : null,
            Sam = includeSam ? BuildSam(cph, samAnimalSpeciesCode, samLocationName, samEmails ?? [], samPhones ?? []) : null
        };
    }

    private static CtsCphHoldingModel BuildCts(LidFullIdentifier lid, string? locationName, string[] emails, string[] phones)
    {
        var keeperData = emails
            .Select(email => new Dictionary<string, object?>
            {
                [DataFields.CtsKeeperFields.ParEmailAddress] = email,
                [DataFields.CtsKeeperFields.ParMobileNumber] = null,
                [DataFields.CtsKeeperFields.ParTelNumber] = null,
            })
            .Concat(phones.Select(phone => new Dictionary<string, object?>
            {
                [DataFields.CtsKeeperFields.ParEmailAddress] = null,
                [DataFields.CtsKeeperFields.ParMobileNumber] = null,
                [DataFields.CtsKeeperFields.ParTelNumber] = phone,
            }))
            .ToList();

        return new CtsCphHoldingModel
        {
            Id = lid,
            Holding = new Dictionary<string, object?>
            {
                [DataFields.CtsCphHoldingFields.LidFullIdentifier] = lid.Value,
                [DataFields.CtsCphHoldingFields.AdrName] = locationName,
                [DataFields.CtsCphHoldingFields.LocMobileNumber] = null,
                [DataFields.CtsCphHoldingFields.LocTelNumber] = null,
            },
            Keepers = new QueryResult { CollectionName = "cts_keeper", Data = keeperData, Count = keeperData.Count }
        };
    }

    private static SamCphHoldingModel BuildSam(Cph cph, string? animalSpeciesCode, string? locationName, string[] emails, string[] phones)
    {
        var partyData = emails.Select(email => new Dictionary<string, object?>
        {
            [DataFields.SamPartyFields.InternetEmailAddress] = email,
            [DataFields.SamPartyFields.TelephoneNumber] = null,
            [DataFields.SamPartyFields.MobileNumber] = null,
        }).ToList();

        var holderData = phones.Select(phone => new Dictionary<string, object?>
        {
            [DataFields.SamCphHolderFields.InternetEmailAddress] = null,
            [DataFields.SamCphHolderFields.TelephoneNumber] = phone,
            [DataFields.SamCphHolderFields.MobileNumber] = null,
        }).ToList();

        return new SamCphHoldingModel
        {
            Cph = cph,
            Holding = new Dictionary<string, object?>
            {
                [DataFields.SamCphHoldingFields.Cph] = cph.Value,
                [DataFields.SamCphHoldingFields.FeatureName] = locationName,
                [DataFields.SamCphHoldingFields.AnimalSpeciesCode] = animalSpeciesCode,
            },
            Herd = new QueryResult { CollectionName = "sam_herd", Data = [], Count = 0 },
            Parties = new QueryResult { CollectionName = "sam_party", Data = partyData, Count = partyData.Count },
            Holders = new QueryResult { CollectionName = "sam_cph_holder", Data = holderData, Count = holderData.Count },
        };
    }
}
