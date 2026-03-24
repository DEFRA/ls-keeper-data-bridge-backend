using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.SamCtsHoldings.Query;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using KeeperData.Core.Throttling;
using KeeperData.Core.Throttling.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using static KeeperData.Core.Reports.SamCtsHoldings.Query.Domain.DataFields;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.SamCtsHoldings.Query;

public class PreloadedCtsSamDataServiceTests
{
    private readonly Mock<IQueryService> _queryServiceMock = new();
    private readonly Mock<IThrottler> _throttlerMock = new();
    private readonly DataSetDefinitions _definitions = StandardDataSetDefinitionsBuilder.Build();
    private readonly PreloadedCtsSamDataService _sut;

    public PreloadedCtsSamDataServiceTests()
    {
        _throttlerMock.Setup(t => t.Settings).Returns(new ThrottlePolicySettings
        {
            CleanseAnalysis = new CleanseAnalysisThrottleSettings { PumpBatchSize = 100, PumpDelayMs = 0 }
        });
        _throttlerMock.Setup(t => t.DelayAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        _sut = new PreloadedCtsSamDataService(
            _definitions,
            _queryServiceMock.Object,
            _throttlerMock.Object,
            NullLogger<PreloadedCtsSamDataService>.Instance);
    }

    // ── PreloadAsync ────────────────────────────────────────────────────────

    [Fact]
    public async Task PreloadAsync_CalledTwice_ThrowsInvalidOperationException()
    {
        SetupEmptyCollections();
        var timings = new TimingTree();
        await _sut.PreloadAsync(timings, CancellationToken.None);

        var act = () => _sut.PreloadAsync(timings, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*already been called*");
    }

    [Fact]
    public async Task PreloadAsync_LoadsAllCollections()
    {
        SetupEmptyCollections();
        var timings = new TimingTree();

        await _sut.PreloadAsync(timings, CancellationToken.None);

        // All 6 collections should have been queried
        _queryServiceMock.Verify(q => q.QueryAsync(
            It.Is<QueryParameters>(p => p.CollectionName == "cts_cph_holding"),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        _queryServiceMock.Verify(q => q.QueryAsync(
            It.Is<QueryParameters>(p => p.CollectionName == "cts_keeper"),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        _queryServiceMock.Verify(q => q.QueryAsync(
            It.Is<QueryParameters>(p => p.CollectionName == "sam_cph_holdings"),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        _queryServiceMock.Verify(q => q.QueryAsync(
            It.Is<QueryParameters>(p => p.CollectionName == "sam_herd"),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        _queryServiceMock.Verify(q => q.QueryAsync(
            It.Is<QueryParameters>(p => p.CollectionName == "sam_party"),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        _queryServiceMock.Verify(q => q.QueryAsync(
            It.Is<QueryParameters>(p => p.CollectionName == "sam_cph_holder"),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [Fact]
    public async Task PreloadAsync_RecordsTimings()
    {
        SetupEmptyCollections();
        var timings = new TimingTree();

        await _sut.PreloadAsync(timings, CancellationToken.None);

        var snapshot = timings.Snapshot("root");
        snapshot.Should().NotBeNull();
    }

    // ── GetCtsCphHolding by LID ─────────────────────────────────────────────

    [Fact]
    public async Task GetCtsCphHolding_ByLid_WhenNotLoaded_ReturnsNull()
    {
        SetupEmptyCollections();
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var result = _sut.GetCtsCphHolding(LidFullIdentifier.Parse("AB-01/234/5678"));

        result.Should().BeNull();
    }

    [Fact]
    public async Task GetCtsCphHolding_ByLid_WhenLoaded_ReturnsModelWithKeepers()
    {
        var lid = "AB-01/234/5678";
        var holdingRow = MakeRow(CtsCphHoldingFields.LidFullIdentifier, lid);
        var keeperRow = MakeRow(CtsKeeperFields.LidFullIdentifier, lid);
        SetupCollection("cts_cph_holding", holdingRow);
        SetupCollection("cts_keeper", keeperRow);
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var result = _sut.GetCtsCphHolding(LidFullIdentifier.Parse(lid));

        result.Should().NotBeNull();
        result!.Holding.Should().BeSameAs(holdingRow);
        result.Keepers.Data.Should().ContainSingle().Which.Should().BeSameAs(keeperRow);
    }

    [Fact]
    public async Task GetCtsCphHolding_ByLid_WithNoKeepers_ReturnsModelWithEmptyKeepers()
    {
        var lid = "AB-01/234/5678";
        SetupCollection("cts_cph_holding", MakeRow(CtsCphHoldingFields.LidFullIdentifier, lid));
        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var result = _sut.GetCtsCphHolding(LidFullIdentifier.Parse(lid));

        result.Should().NotBeNull();
        result!.Keepers.Data.Should().BeEmpty();
    }

    // ── GetCtsCphHolding by CPH ─────────────────────────────────────────────

    [Fact]
    public async Task GetCtsCphHolding_ByCph_WhenNotLoaded_ReturnsNull()
    {
        SetupEmptyCollections();
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var result = _sut.GetCtsCphHolding(Cph.Parse("01/234/5678"));

        result.Should().BeNull();
    }

    [Fact]
    public async Task GetCtsCphHolding_ByCph_WhenLoaded_ReturnsModel()
    {
        var lid = "AB-01/234/5678";
        var holdingRow = MakeRow(CtsCphHoldingFields.LidFullIdentifier, lid);
        SetupCollection("cts_cph_holding", holdingRow);
        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var result = _sut.GetCtsCphHolding(Cph.Parse("01/234/5678"));

        result.Should().NotBeNull();
        result!.Id.Value.Should().Be(lid);
    }

    [Fact]
    public async Task GetCtsCphHolding_ByCph_WhenLidParsingFails_ReturnsNull()
    {
        // Holding row with an invalid LID
        var holdingRow = MakeRow(CtsCphHoldingFields.LidFullIdentifier, "INVALID_LID");
        SetupCollection("cts_cph_holding", holdingRow);
        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        // The row won't be indexed because LidFullIdentifier.TryParse returns null for invalid LID
        // so CPH lookup will miss it
        _sut.GetCtsCphHoldingsCount().Should().Be(1); // raw count still counts
        // but by CPH lookup fails because Cph was never indexed
        var result = _sut.GetCtsCphHolding(Cph.Parse("01/234/5678"));
        result.Should().BeNull();
    }

    // ── GetCtsCphHoldingsCount ──────────────────────────────────────────────

    [Fact]
    public async Task GetCtsCphHoldingsCount_ReturnsLoadedCount()
    {
        var lid1 = "AB-01/234/5678";
        var lid2 = "CD-02/345/6789";
        SetupCollection("cts_cph_holding",
            MakeRow(CtsCphHoldingFields.LidFullIdentifier, lid1),
            MakeRow(CtsCphHoldingFields.LidFullIdentifier, lid2));
        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        _sut.GetCtsCphHoldingsCount().Should().Be(2);
    }

    // ── GetSamCphHolding ────────────────────────────────────────────────────

    [Fact]
    public async Task GetSamCphHolding_WhenNotLoaded_ReturnsNull()
    {
        SetupEmptyCollections();
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var result = _sut.GetSamCphHolding(Cph.Parse("01/234/5678"));

        result.Should().BeNull();
    }

    [Fact]
    public async Task GetSamCphHolding_WhenLoaded_ReturnsModelWithHerdsPartiesAndHolders()
    {
        var cph = "01/234/5678";
        var holdingRow = MakeRow(SamCphHoldingFields.Cph, cph);
        var herdRow = MakeRow(
            SamHerd.Cphh, $"{cph}/01",
            SamHerd.OwnerPartyIds, "P1,P2",
            SamHerd.KeeperPartyIds, "P3");
        var partyP1 = MakeRow(SamPartyFields.PartyId, "P1");
        var partyP2 = MakeRow(SamPartyFields.PartyId, "P2");
        var partyP3 = MakeRow(SamPartyFields.PartyId, "P3");
        var holderRow = MakeRow(SamCphHolderFields.Cphs, cph);

        // Also need a CTS holding so the holder CPH matching works
        var lid = "AB-01/234/5678";
        SetupCollection("cts_cph_holding", MakeRow(CtsCphHoldingFields.LidFullIdentifier, lid));
        SetupEmptyCollection("cts_keeper");
        SetupCollection("sam_cph_holdings", holdingRow);
        SetupCollection("sam_herd", herdRow);
        SetupCollection("sam_party", partyP1, partyP2, partyP3);
        SetupCollection("sam_cph_holder", holderRow);
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var result = _sut.GetSamCphHolding(Cph.Parse(cph));

        result.Should().NotBeNull();
        result!.Cph.Value.Should().Be(cph);
        result.Holding.Should().BeSameAs(holdingRow);
        result.Herd.Data.Should().HaveCount(1);
        result.Parties.Data.Should().HaveCount(3);
        result.Holders.Data.Should().HaveCount(1);
    }

    [Fact]
    public async Task GetSamCphHolding_WhenHerdHasNoPartyIds_PartiesAreEmpty()
    {
        var cph = "01/234/5678";
        var holdingRow = MakeRow(SamCphHoldingFields.Cph, cph);
        // Herd with blank/whitespace-only party IDs
        var herdRow = MakeRow(
            SamHerd.Cphh, $"{cph}/01",
            SamHerd.OwnerPartyIds, " ",
            SamHerd.KeeperPartyIds, "");

        SetupEmptyCollection("cts_cph_holding");
        SetupEmptyCollection("cts_keeper");
        SetupCollection("sam_cph_holdings", holdingRow);
        SetupCollection("sam_herd", herdRow);
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var result = _sut.GetSamCphHolding(Cph.Parse(cph));

        result.Should().NotBeNull();
        result!.Parties.Data.Should().BeEmpty();
    }

    // ── GetSamCphHoldingsCount ──────────────────────────────────────────────

    [Fact]
    public async Task GetSamCphHoldingsCount_ReturnsLoadedCount()
    {
        SetupEmptyCollection("cts_cph_holding");
        SetupEmptyCollection("cts_keeper");
        SetupCollection("sam_cph_holdings",
            MakeRow(SamCphHoldingFields.Cph, "01/001/0001"),
            MakeRow(SamCphHoldingFields.Cph, "02/002/0002"));
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        _sut.GetSamCphHoldingsCount().Should().Be(2);
    }

    // ── ListCtsCphHoldings ──────────────────────────────────────────────────

    [Fact]
    public async Task ListCtsCphHoldings_ReturnsPage()
    {
        var lid1 = "AB-01/001/0001";
        var lid2 = "CD-02/002/0002";
        var lid3 = "EF-03/003/0003";
        SetupCollection("cts_cph_holding",
            MakeRow(CtsCphHoldingFields.LidFullIdentifier, lid1),
            MakeRow(CtsCphHoldingFields.LidFullIdentifier, lid2),
            MakeRow(CtsCphHoldingFields.LidFullIdentifier, lid3));
        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var page = _sut.ListCtsCphHoldings(skip: 1, take: 2);

        page.CollectionName.Should().Be("cts_cph_holding");
        page.Data.Should().HaveCount(2);
        page.TotalCount.Should().Be(3);
        page.Skip.Should().Be(1);
        page.Top.Should().Be(2);
    }

    [Fact]
    public async Task ListCtsCphHoldings_TakeBeyondEnd_ClampedToAvailable()
    {
        SetupCollection("cts_cph_holding",
            MakeRow(CtsCphHoldingFields.LidFullIdentifier, "AB-01/001/0001"));
        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var page = _sut.ListCtsCphHoldings(skip: 0, take: 100);

        page.Data.Should().HaveCount(1);
        page.TotalCount.Should().Be(1);
    }

    [Fact]
    public async Task ListCtsCphHoldings_SkipBeyondEnd_ReturnsEmpty()
    {
        SetupCollection("cts_cph_holding",
            MakeRow(CtsCphHoldingFields.LidFullIdentifier, "AB-01/001/0001"));
        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var page = _sut.ListCtsCphHoldings(skip: 10, take: 5);

        page.Data.Should().BeEmpty();
    }

    // ── ListSamCphHoldings ──────────────────────────────────────────────────

    [Fact]
    public async Task ListSamCphHoldings_ReturnsPage()
    {
        SetupEmptyCollection("cts_cph_holding");
        SetupEmptyCollection("cts_keeper");
        SetupCollection("sam_cph_holdings",
            MakeRow(SamCphHoldingFields.Cph, "01/001/0001"),
            MakeRow(SamCphHoldingFields.Cph, "02/002/0002"));
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var page = _sut.ListSamCphHoldings(skip: 0, take: 10);

        page.CollectionName.Should().Be("sam_cph_holdings");
        page.Data.Should().HaveCount(2);
        page.TotalCount.Should().Be(2);
    }

    // ── Paging infrastructure ───────────────────────────────────────────────

    [Fact]
    public async Task PreloadAsync_PagesThroughMultipleBatches()
    {
        // Batch size is 100; provide 2 batches of CTS holdings
        var batch1 = Enumerable.Range(1, 100)
            .Select(i => MakeRow(CtsCphHoldingFields.LidFullIdentifier, $"AB-{i:D2}/{i:D3}/{i:D4}"))
            .ToList();
        var batch2 = Enumerable.Range(101, 50)
            .Select(i => MakeRow(CtsCphHoldingFields.LidFullIdentifier, $"AB-{i:D2}/{i:D3}/{i:D4}"))
            .ToList();

        var callCount = 0;
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "cts_cph_holding"),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                var batch = callCount switch
                {
                    0 => batch1,
                    1 => batch2,
                    _ => []
                };
                callCount++;
                return MakeResult("cts_cph_holding", batch);
            });

        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        _sut.GetCtsCphHoldingsCount().Should().Be(150);
    }

    // ── CTS Keeper indexing ─────────────────────────────────────────────────

    [Fact]
    public async Task PreloadAsync_SkipsKeepersWithNoLid()
    {
        SetupEmptyCollection("cts_cph_holding");
        // Keeper with no LID — should be skipped
        SetupCollection("cts_keeper", MakeRow("SOME_FIELD", "value"));
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        // No error, just skipped
        _sut.GetCtsCphHoldingsCount().Should().Be(0);
    }

    // ── SAM Herd CPHH parsing ───────────────────────────────────────────────

    [Fact]
    public async Task PreloadAsync_SkipsHerdsWithInvalidCphh()
    {
        var cph = "01/234/5678";
        var holdingRow = MakeRow(SamCphHoldingFields.Cph, cph);
        // Herd with invalid/empty CPHH — should be skipped
        var badHerd = MakeRow(SamHerd.Cphh, "");
        SetupEmptyCollection("cts_cph_holding");
        SetupEmptyCollection("cts_keeper");
        SetupCollection("sam_cph_holdings", holdingRow);
        SetupCollection("sam_herd", badHerd);
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var result = _sut.GetSamCphHolding(Cph.Parse(cph));
        result.Should().NotBeNull();
        result!.Herd.Data.Should().BeEmpty();
    }

    // ── SAM CPH Holders ─────────────────────────────────────────────────────

    [Fact]
    public async Task PreloadAsync_IndexesSamCphHoldersAcrossMultipleCphs()
    {
        var cph1 = "01/001/0001";
        var cph2 = "02/002/0002";
        var lid = "AB-01/001/0001";
        SetupCollection("cts_cph_holding", MakeRow(CtsCphHoldingFields.LidFullIdentifier, lid));
        SetupEmptyCollection("cts_keeper");
        SetupCollection("sam_cph_holdings", MakeRow(SamCphHoldingFields.Cph, cph2));
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        // Holder mapped to two CPHs (one from CTS, one from SAM)
        SetupCollection("sam_cph_holder", MakeRow(SamCphHolderFields.Cphs, $"{cph1},{cph2}"));
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        var result = _sut.GetSamCphHolding(Cph.Parse(cph2));
        result.Should().NotBeNull();
        result!.Holders.Data.Should().HaveCount(1);
    }

    [Fact]
    public async Task PreloadAsync_SkipsHoldersWithEmptyCphs()
    {
        SetupEmptyCollection("cts_cph_holding");
        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        // Holder with empty CPHS
        SetupCollection("sam_cph_holder", MakeRow(SamCphHolderFields.Cphs, ""));
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        // Should not throw, just skip
        _sut.GetSamCphHoldingsCount().Should().Be(0);
    }

    [Fact]
    public async Task PreloadAsync_HolderWithUnknownCph_IsNotIndexed()
    {
        SetupEmptyCollection("cts_cph_holding");
        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        // Holder referencing a CPH not in any CTS/SAM holding
        SetupCollection("sam_cph_holder", MakeRow(SamCphHolderFields.Cphs, "99/999/9999"));
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        // No holdings, so no holder mappings
        _sut.GetCtsCphHoldingsCount().Should().Be(0);
        _sut.GetSamCphHoldingsCount().Should().Be(0);
    }

    // ── SAM Parties ─────────────────────────────────────────────────────────

    [Fact]
    public async Task PreloadAsync_SkipsPartiesWithNoPartyId()
    {
        SetupEmptyCollection("cts_cph_holding");
        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        // Party with no PARTY_ID — skipped
        SetupCollection("sam_party", MakeRow("SOME_FIELD", "value"));
        SetupEmptyCollection("sam_cph_holder");
        await _sut.PreloadAsync(new TimingTree(), CancellationToken.None);

        // No error
        _sut.GetSamCphHoldingsCount().Should().Be(0);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static Dictionary<string, object?> MakeRow(params string[] keyValues)
    {
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < keyValues.Length; i += 2)
            dict[keyValues[i]] = keyValues[i + 1];
        return dict;
    }

    private static Dictionary<string, object?> MakeRow(string key, string value)
        => new(StringComparer.OrdinalIgnoreCase) { [key] = value };

    private static QueryResult MakeResult(string collection, List<Dictionary<string, object?>> data) => new()
    {
        CollectionName = collection,
        Data = data,
        Count = data.Count
    };

    private void SetupEmptyCollections()
    {
        SetupEmptyCollection("cts_cph_holding");
        SetupEmptyCollection("cts_keeper");
        SetupEmptyCollection("sam_cph_holdings");
        SetupEmptyCollection("sam_herd");
        SetupEmptyCollection("sam_party");
        SetupEmptyCollection("sam_cph_holder");
    }

    private void SetupEmptyCollection(string collectionName)
    {
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == collectionName),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(MakeResult(collectionName, []));
    }

    private void SetupCollection(string collectionName, params Dictionary<string, object?>[] rows)
    {
        var data = rows.ToList();
        var returned = false;
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == collectionName),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                if (!returned)
                {
                    returned = true;
                    return MakeResult(collectionName, data);
                }
                return MakeResult(collectionName, []);
            });
    }
}
