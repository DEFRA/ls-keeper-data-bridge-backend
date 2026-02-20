using FluentAssertions;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Impl;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using KeeperData.Core.Reports.Issues.Command.Requests;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using Moq;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Analysis.Command.Impl;

public class CleanseAnalysisEngineTests
{
    private readonly Mock<ICtsSamQueryService> _dataServiceMock = new();
    private readonly Mock<IIssueCommandService> _issueServiceMock = new();
    private readonly CleanseAnalysisEngine _sut;

    private const string OperationId = "op-1";

    public CleanseAnalysisEngineTests()
    {
        _sut = new CleanseAnalysisEngine(_dataServiceMock.Object, _issueServiceMock.Object);
        _issueServiceMock.Setup(s => s.RecordIssueAsync(It.IsAny<RecordIssueCommand>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(IssueRecordResult.Created);
    }

    [Fact]
    public async Task Execute_CtsHoldingNotInSam_ShouldRaiseRule2A()
    {
        SetupCtsHoldings("UK-12/345/6001");
        SetupSamHoldings();
        _dataServiceMock.Setup(s => s.GetSamCphHoldingAsync(CphFor("12/345/6001"), It.IsAny<CancellationToken>()))
            .ReturnsAsync((SamCphHoldingModel?)null);

        await RunEngineAsync();

        VerifyIssueRecorded(RuleIds.CTS_CPH_NOT_IN_SAM);
    }

    [Fact]
    public async Task Execute_SamHoldingNotInCts_ShouldRaiseRule2B()
    {
        SetupCtsHoldings();
        SetupSamHoldings("12/345/6002");
        _dataServiceMock.Setup(s => s.GetCtsCphHoldingAsync(CphFor("12/345/6002"), It.IsAny<CancellationToken>()))
            .ReturnsAsync((CtsCphHoldingModel?)null);

        await RunEngineAsync();

        VerifyIssueRecorded(RuleIds.SAM_CPH_NOT_IN_CTS);
    }

    [Fact]
    public async Task Execute_NoEmailsInEitherSystem_ShouldRaiseRule4()
    {
        SetupCtsHoldings("UK-12/345/6003");
        SetupSamHoldings();
        SetupMatchingPair("12/345/6003", ctsEmails: [], samEmails: [], ctsPhones: ["01234"], samPhones: ["01234"]);

        await RunEngineAsync();

        VerifyIssueRecorded(RuleIds.CTS_SAM_NO_EMAIL_ADDRESSES);
    }

    [Fact]
    public async Task Execute_NoPhoneNumbersInEitherSystem_ShouldRaiseRule5()
    {
        SetupCtsHoldings("UK-12/345/6003");
        SetupSamHoldings();
        SetupMatchingPair("12/345/6003", ctsEmails: ["a@b.com"], samEmails: ["a@b.com"], ctsPhones: [], samPhones: []);

        await RunEngineAsync();

        VerifyIssueRecorded(RuleIds.CTS_SAM_NO_PHONE_NUMBERS);
    }

    [Fact]
    public async Task Execute_CtsEmailMissingFromSam_ShouldRaiseRule12()
    {
        SetupCtsHoldings("UK-12/345/6003");
        SetupSamHoldings();
        SetupMatchingPair("12/345/6003", ctsEmails: ["a@b.com", "c@d.com"], samEmails: ["a@b.com"], ctsPhones: ["01234"], samPhones: ["01234"]);

        await RunEngineAsync();

        VerifyIssueRecorded(RuleIds.SAM_MISSING_EMAIL_ADDRESSES);
    }

    [Fact]
    public async Task Execute_CtsPhoneMissingFromSam_ShouldRaiseRule11()
    {
        SetupCtsHoldings("UK-12/345/6003");
        SetupSamHoldings();
        SetupMatchingPair("12/345/6003", ctsEmails: ["a@b.com"], samEmails: ["a@b.com"], ctsPhones: ["01234", "05678"], samPhones: ["01234"]);

        await RunEngineAsync();

        VerifyIssueRecorded(RuleIds.SAM_MISSING_PHONE_NUMBERS);
    }

    [Fact]
    public async Task Execute_SamAnimalSpeciesNotCtt_ShouldRaiseRule1()
    {
        SetupCtsHoldings("UK-12/345/6003");
        SetupSamHoldings();
        SetupMatchingPair("12/345/6003", ctsEmails: ["a@b.com"], samEmails: ["a@b.com"], ctsPhones: ["01234"], samPhones: ["01234"],
            samAnimalSpeciesCode: "SHP");

        await RunEngineAsync();

        VerifyIssueRecorded(RuleIds.SAM_NO_CATTLE_UNIT);
    }

    [Fact]
    public async Task Execute_SamCttWithDifferentLocation_ShouldRaiseRule3()
    {
        SetupCtsHoldings("UK-12/345/6003");
        SetupSamHoldings();
        SetupMatchingPair("12/345/6003", ctsEmails: ["a@b.com"], samEmails: ["a@b.com"], ctsPhones: ["01234"], samPhones: ["01234"],
            samAnimalSpeciesCode: "CTT", ctsLocationName: "CTS Farm", samLocationName: "SAM Farm");

        await RunEngineAsync();

        VerifyIssueRecorded(RuleIds.SAM_CATTLE_RELATED_CPHs);
    }

    [Fact]
    public async Task Execute_SamCttWithUnknownLocation_ShouldRaiseRule3()
    {
        SetupCtsHoldings("UK-12/345/6003");
        SetupSamHoldings();
        SetupMatchingPair("12/345/6003", ctsEmails: ["a@b.com"], samEmails: ["a@b.com"], ctsPhones: ["01234"], samPhones: ["01234"],
            samAnimalSpeciesCode: "CTT", ctsLocationName: "CTS Farm", samLocationName: "Unknown");

        await RunEngineAsync();

        VerifyIssueRecorded(RuleIds.SAM_CATTLE_RELATED_CPHs);
    }

    [Fact]
    public async Task Execute_AllDataMatching_ShouldNotRaiseAnyIssues()
    {
        SetupCtsHoldings("UK-12/345/6003");
        SetupSamHoldings();
        SetupMatchingPair("12/345/6003", ctsEmails: ["a@b.com"], samEmails: ["a@b.com"], ctsPhones: ["01234"], samPhones: ["01234"],
            samAnimalSpeciesCode: "CTT", ctsLocationName: "Same Farm", samLocationName: "Same Farm");

        await RunEngineAsync();

        _issueServiceMock.Verify(s => s.RecordIssueAsync(It.IsAny<RecordIssueCommand>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Execute_InvalidCountyCode_ShouldSkipCtsRecord()
    {
        // County code 99 exceeds the 1-51 valid range
        SetupCtsHoldings("UK-99/345/6001");
        SetupSamHoldings();

        await RunEngineAsync();

        _issueServiceMock.Verify(s => s.RecordIssueAsync(It.IsAny<RecordIssueCommand>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    #region Helpers

    private static Cph CphFor(string value) => Cph.Parse(value);

    private void SetupCtsHoldings(params string[] lidFullIdentifiers)
    {
        var data = lidFullIdentifiers.Select(lid => new Dictionary<string, object?>
        {
            [DataFields.CtsCphHoldingFields.LidFullIdentifier] = lid
        }).ToList();

        _dataServiceMock.Setup(s => s.GetCtsCphHoldingsCountAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(data.Count);
        _dataServiceMock.Setup(s => s.ListCtsCphHoldingsAsync(0, It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { CollectionName = "cts_cph_holding", Data = data, Count = data.Count });
        _dataServiceMock.Setup(s => s.ListCtsCphHoldingsAsync(It.Is<int>(skip => skip > 0), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { CollectionName = "cts_cph_holding", Data = [], Count = 0 });
    }

    private void SetupSamHoldings(params string[] cphs)
    {
        var data = cphs.Select(cph => new Dictionary<string, object?>
        {
            [DataFields.SamCphHoldingFields.Cph] = cph
        }).ToList();

        _dataServiceMock.Setup(s => s.GetSamCphHoldingsCountAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(data.Count);
        _dataServiceMock.Setup(s => s.ListSamCphHoldingsAsync(0, It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { CollectionName = "sam_cph_holdings", Data = data, Count = data.Count });
        _dataServiceMock.Setup(s => s.ListSamCphHoldingsAsync(It.Is<int>(skip => skip > 0), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { CollectionName = "sam_cph_holdings", Data = [], Count = 0 });
    }

    private void SetupMatchingPair(
        string cphValue,
        string[] ctsEmails,
        string[] samEmails,
        string[] ctsPhones,
        string[] samPhones,
        string samAnimalSpeciesCode = "CTT",
        string ctsLocationName = "Farm",
        string samLocationName = "Farm")
    {
        var cph = CphFor(cphValue);
        var lid = LidFullIdentifier.Parse($"UK-{cphValue}");

        // CTS holding
        var ctsHoldingPhones = ctsPhones.Take(1).ToArray();
        var ctsKeeperPhones = ctsPhones.Skip(1).ToArray();
        var keeperData = new List<Dictionary<string, object?>>();

        // Add keepers with emails
        foreach (var email in ctsEmails)
        {
            keeperData.Add(new Dictionary<string, object?>
            {
                [DataFields.CtsKeeperFields.ParEmailAddress] = email,
                [DataFields.CtsKeeperFields.ParMobileNumber] = null,
                [DataFields.CtsKeeperFields.ParTelNumber] = null,
            });
        }

        // Add keeper with phone if any keeper-specific phones exist
        foreach (var phone in ctsKeeperPhones)
        {
            keeperData.Add(new Dictionary<string, object?>
            {
                [DataFields.CtsKeeperFields.ParEmailAddress] = null,
                [DataFields.CtsKeeperFields.ParMobileNumber] = null,
                [DataFields.CtsKeeperFields.ParTelNumber] = phone,
            });
        }

        var ctsModel = new CtsCphHoldingModel
        {
            Id = lid,
            Holding = new Dictionary<string, object?>
            {
                [DataFields.CtsCphHoldingFields.LidFullIdentifier] = lid.Value,
                [DataFields.CtsCphHoldingFields.AdrName] = ctsLocationName,
                [DataFields.CtsCphHoldingFields.LocMobileNumber] = null,
                [DataFields.CtsCphHoldingFields.LocTelNumber] = ctsHoldingPhones.Length > 0 ? ctsHoldingPhones[0] : null,
            },
            Keepers = new QueryResult
            {
                CollectionName = "cts_keeper",
                Data = keeperData,
                Count = keeperData.Count,
            }
        };

        _dataServiceMock.Setup(s => s.GetCtsCphHoldingAsync(It.Is<LidFullIdentifier>(l => l.Value == lid.Value), It.IsAny<CancellationToken>()))
            .ReturnsAsync(ctsModel);

        // SAM holding
        var samPartyData = samEmails.Select(e => new Dictionary<string, object?>
        {
            [DataFields.SamPartyFields.InternetEmailAddress] = e,
            [DataFields.SamPartyFields.TelephoneNumber] = null,
            [DataFields.SamPartyFields.MobileNumber] = null,
        }).ToList();

        var samHolderData = samPhones.Select(p => new Dictionary<string, object?>
        {
            [DataFields.SamCphHolderFields.InternetEmailAddress] = null,
            [DataFields.SamCphHolderFields.TelephoneNumber] = p,
            [DataFields.SamCphHolderFields.MobileNumber] = null,
        }).ToList();

        var samModel = new SamCphHoldingModel
        {
            Cph = cph,
            Holding = new Dictionary<string, object?>
            {
                [DataFields.SamCphHoldingFields.Cph] = cph.Value,
                [DataFields.SamCphHoldingFields.FeatureName] = samLocationName,
                [DataFields.SamCphHoldingFields.AnimalSpeciesCode] = samAnimalSpeciesCode,
            },
            Herd = new QueryResult { CollectionName = "sam_herd", Data = [], Count = 0 },
            Parties = new QueryResult { CollectionName = "sam_party", Data = samPartyData, Count = samPartyData.Count },
            Holders = new QueryResult { CollectionName = "sam_cph_holder", Data = samHolderData, Count = samHolderData.Count },
        };

        _dataServiceMock.Setup(s => s.GetSamCphHoldingAsync(It.Is<Cph>(c => c.Value == cph.Value), It.IsAny<CancellationToken>()))
            .ReturnsAsync(samModel);
    }

    private async Task RunEngineAsync()
    {
        await _sut.ExecuteAsync(OperationId, (_, _, _, _) => Task.CompletedTask, CancellationToken.None);
    }

    private void VerifyIssueRecorded(string ruleId)
    {
        _issueServiceMock.Verify(s => s.RecordIssueAsync(
            It.Is<RecordIssueCommand>(c => c.Descriptor.RuleId == ruleId),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    #endregion
}
