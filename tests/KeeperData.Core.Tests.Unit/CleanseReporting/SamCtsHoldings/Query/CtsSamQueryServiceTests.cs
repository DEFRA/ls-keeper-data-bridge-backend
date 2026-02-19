using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.SamCtsHoldings.Query;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using Moq;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.SamCtsHoldings.Query;

public class CtsSamQueryServiceTests
{
    private readonly Mock<IQueryService> _queryServiceMock = new();
    private readonly DataSetDefinitions _definitions = StandardDataSetDefinitionsBuilder.Build();
    private readonly CtsSamQueryService _sut;

    public CtsSamQueryServiceTests()
    {
        _sut = new CtsSamQueryService(_definitions, _queryServiceMock.Object);
    }

    [Fact]
    public async Task GetCtsCphHoldingsCountAsync_ShouldQueryWithCountOnly()
    {
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "cts_cph_holding" && p.Top == 0 && p.IncludeCount),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(EmptyResult("cts_cph_holding", totalCount: 42));

        var count = await _sut.GetCtsCphHoldingsCountAsync(CancellationToken.None);

        count.Should().Be(42);
    }

    [Fact]
    public async Task GetSamCphHoldingsCountAsync_ShouldQueryWithCountOnly()
    {
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "sam_cph_holdings" && p.Top == 0 && p.IncludeCount),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(EmptyResult("sam_cph_holdings", totalCount: 10));

        var count = await _sut.GetSamCphHoldingsCountAsync(CancellationToken.None);

        count.Should().Be(10);
    }

    [Fact]
    public async Task ListCtsCphHoldingsAsync_ShouldSelectOnlyLidFullIdentifier()
    {
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p =>
                    p.CollectionName == "cts_cph_holding" &&
                    p.Skip == 5 && p.Top == 10 &&
                    p.FieldsToSelect != null && p.FieldsToSelect.Contains("LID_FULL_IDENTIFIER")),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(EmptyResult("cts_cph_holding"));

        await _sut.ListCtsCphHoldingsAsync(5, 10, CancellationToken.None);

        _queryServiceMock.Verify(q => q.QueryAsync(
            It.Is<QueryParameters>(p => p.FieldsToSelect!.Contains("LID_FULL_IDENTIFIER")),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ListSamCphHoldingsAsync_ShouldSelectOnlyCph()
    {
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p =>
                    p.CollectionName == "sam_cph_holdings" &&
                    p.FieldsToSelect != null && p.FieldsToSelect.Contains("CPH")),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(EmptyResult("sam_cph_holdings"));

        await _sut.ListSamCphHoldingsAsync(0, 100, CancellationToken.None);

        _queryServiceMock.Verify(q => q.QueryAsync(
            It.Is<QueryParameters>(p => p.FieldsToSelect!.Contains("CPH")),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetCtsCphHoldingAsync_ByLid_WhenFound_ShouldReturnModelWithKeepers()
    {
        var lid = LidFullIdentifier.Parse("UK-12/345/6789");
        var holdingData = new Dictionary<string, object?>
        {
            [DataFields.CtsCphHoldingFields.LidFullIdentifier] = "UK-12/345/6789",
            [DataFields.CtsCphHoldingFields.AdrName] = "Test Farm",
        };

        // Holding query
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "cts_cph_holding" && p.Top == 1),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { CollectionName = "cts_cph_holding", Data = [holdingData], Count = 1 });

        // Keepers query
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "cts_keeper"),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(EmptyResult("cts_keeper"));

        var result = await _sut.GetCtsCphHoldingAsync(lid, CancellationToken.None);

        result.Should().NotBeNull();
        result!.Id.Value.Should().Be("UK-12/345/6789");
        result.Keepers.Should().NotBeNull();
    }

    [Fact]
    public async Task GetCtsCphHoldingAsync_ByLid_WhenNotFound_ShouldReturnNull()
    {
        var lid = LidFullIdentifier.Parse("UK-12/345/6789");

        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "cts_cph_holding"),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(EmptyResult("cts_cph_holding"));

        var result = await _sut.GetCtsCphHoldingAsync(lid, CancellationToken.None);

        result.Should().BeNull();
    }

    [Fact]
    public async Task GetCtsCphHoldingAsync_ByCph_ShouldUseEndsWithFilter()
    {
        var cph = Cph.Parse("12/345/6789");

        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "cts_cph_holding"),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(EmptyResult("cts_cph_holding"));

        await _sut.GetCtsCphHoldingAsync(cph, CancellationToken.None);

        _queryServiceMock.Verify(q => q.QueryAsync(
            It.Is<QueryParameters>(p =>
                p.Filter != null && p.Filter.ToString()!.Contains("-12/345/6789")),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetSamCphHoldingAsync_WhenFound_ShouldAssembleFullModel()
    {
        var cph = Cph.Parse("12/345/6789");

        // SAM holding
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "sam_cph_holdings" && p.Top == 1),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult
            {
                CollectionName = "sam_cph_holdings",
                Data = [new Dictionary<string, object?> { [DataFields.SamCphHoldingFields.Cph] = "12/345/6789" }],
                Count = 1
            });

        // SAM herd with party IDs
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "sam_herd"),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult
            {
                CollectionName = "sam_herd",
                Data = [new Dictionary<string, object?>
                {
                    [DataFields.SamHerd.OwnerPartyIds] = "P001,P002",
                    [DataFields.SamHerd.KeeperPartyIds] = "P002"
                }],
                Count = 1
            });

        // SAM party queries (P001 and P002 - deduplicated)
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "sam_party"),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(EmptyResult("sam_party"));

        // SAM holders
        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "sam_cph_holder"),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(EmptyResult("sam_cph_holder"));

        var result = await _sut.GetSamCphHoldingAsync(cph, CancellationToken.None);

        result.Should().NotBeNull();
        result!.Cph.Value.Should().Be("12/345/6789");
        result.Herd.Should().NotBeNull();
        result.Parties.Should().NotBeNull();
        result.Holders.Should().NotBeNull();

        // Verify party queries were made for deduplicated IDs (P001 and P002)
        _queryServiceMock.Verify(q => q.QueryAsync(
            It.Is<QueryParameters>(p => p.CollectionName == "sam_party"),
            It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task GetSamCphHoldingAsync_WhenNotFound_ShouldReturnNull()
    {
        var cph = Cph.Parse("12/345/6789");

        _queryServiceMock.Setup(q => q.QueryAsync(
                It.Is<QueryParameters>(p => p.CollectionName == "sam_cph_holdings"),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(EmptyResult("sam_cph_holdings"));

        var result = await _sut.GetSamCphHoldingAsync(cph, CancellationToken.None);

        result.Should().BeNull();
    }

    [Fact]
    public async Task AllQueries_ShouldIncludeIsDeletedFalseFilter()
    {
        _queryServiceMock.Setup(q => q.QueryAsync(It.IsAny<QueryParameters>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(EmptyResult("test"));

        await _sut.GetCtsCphHoldingsCountAsync(CancellationToken.None);

        _queryServiceMock.Verify(q => q.QueryAsync(
            It.Is<QueryParameters>(p => p.Filter != null && p.Filter.ToString()!.Contains("IsDeleted")),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    private static QueryResult EmptyResult(string collection, long? totalCount = null) => new()
    {
        CollectionName = collection,
        Data = [],
        Count = 0,
        TotalCount = totalCount
    };
}
