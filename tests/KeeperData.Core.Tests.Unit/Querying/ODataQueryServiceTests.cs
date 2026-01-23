using FluentAssertions;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Impl;
using KeeperData.Core.Querying.Models;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Core.Tests.Unit.Querying;

public class ODataQueryServiceTests
{
    private readonly Mock<IQueryService> _queryServiceMock;
    private readonly Mock<ILogger<ODataQueryService>> _loggerMock;
    private readonly ODataQueryService _sut;

    public ODataQueryServiceTests()
    {
        _queryServiceMock = new Mock<IQueryService>();
        _loggerMock = new Mock<ILogger<ODataQueryService>>();

        _sut = new ODataQueryService(
            _queryServiceMock.Object,
            _loggerMock.Object);
    }

    [Fact]
    public async Task QueryAsync_WithValidParameters_CallsQueryService()
    {
        // Arrange
        var expectedResult = new QueryResult
        {
            CollectionName = "sam_cph_holdings",
            Data = new List<Dictionary<string, object?>>(),
            Count = 0,
            TotalCount = 0,
            Skip = 0,
            Top = 100,
            Filter = "CPH eq 'ABC123'",
            OrderBy = "UpdatedAtUtc desc",
            Select = "CPH,UpdatedAtUtc",
            ExecutedAtUtc = DateTime.UtcNow
        };

        _queryServiceMock
            .Setup(x => x.QueryAsync(It.IsAny<QueryParameters>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult
            {
                CollectionName = "sam_cph_holdings",
                Data = new List<Dictionary<string, object?>>(),
                Count = 0,
                TotalCount = 0,
                Skip = 0,
                Top = 100,
                ExecutedAtUtc = expectedResult.ExecutedAtUtc
            });

        // Act
        var result = await _sut.QueryAsync(
            "sam_cph_holdings",
            filter: "CPH eq 'ABC123'",
            orderBy: "UpdatedAtUtc desc",
            select: "CPH,UpdatedAtUtc",
            skip: 0,
            top: 100,
            count: true,
            CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.CollectionName.Should().Be("sam_cph_holdings");
        result.Filter.Should().Be("CPH eq 'ABC123'");
        result.OrderBy.Should().Be("UpdatedAtUtc desc");
        result.Select.Should().Be("CPH,UpdatedAtUtc");

        _queryServiceMock.Verify(
            x => x.QueryAsync(
                It.Is<QueryParameters>(p =>
                    p.CollectionName == "sam_cph_holdings" &&
                    p.Skip == 0 &&
                    p.Top == 100 &&
                    p.IncludeCount == true &&
                    p.Filter != null &&
                    p.Sort != null &&
                    p.FieldsToSelect != null),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task QueryAsync_WithNullODataParameters_PassesNullToQueryService()
    {
        // Arrange
        _queryServiceMock
            .Setup(x => x.QueryAsync(It.IsAny<QueryParameters>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult
            {
                CollectionName = "sam_cph_holdings",
                Data = new List<Dictionary<string, object?>>(),
                Count = 0,
                TotalCount = 0,
                Skip = 0,
                Top = 100,
                ExecutedAtUtc = DateTime.UtcNow
            });

        // Act
        var result = await _sut.QueryAsync(
            "sam_cph_holdings",
            filter: null,
            orderBy: null,
            select: null,
            skip: null,
            top: null,
            count: false,
            CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.Filter.Should().BeNull();
        result.OrderBy.Should().BeNull();
        result.Select.Should().BeNull();

        _queryServiceMock.Verify(
            x => x.QueryAsync(
                It.Is<QueryParameters>(p =>
                    p.CollectionName == "sam_cph_holdings" &&
                    p.Filter == null &&
                    p.Sort == null &&
                    p.FieldsToSelect == null &&
                    p.Skip == 0 &&
                    p.Top == 0 &&
                    p.IncludeCount == false),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task QueryAsync_WithInvalidFilter_ThrowsArgumentException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _sut.QueryAsync(
                "sam_cph_holdings",
                filter: "invalid filter expression @@##",
                orderBy: null,
                select: null,
                skip: 0,
                top: 10,
                count: true,
                CancellationToken.None));
    }

    [Fact]
    public async Task QueryAsync_WithInvalidOrderBy_ThrowsArgumentException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _sut.QueryAsync(
                "sam_cph_holdings",
                filter: null,
                orderBy: "invalid@orderby",
                select: null,
                skip: 0,
                top: 10,
                count: true,
                CancellationToken.None));
    }

    [Fact]
    public async Task QueryAsync_WithInvalidSelect_ThrowsArgumentException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _sut.QueryAsync(
                "sam_cph_holdings",
                filter: null,
                orderBy: null,
                select: "invalid@select,123field",
                skip: 0,
                top: 10,
                count: true,
                CancellationToken.None));
    }

    [Fact]
    public async Task QueryAsync_PreservesODataStringsInResult()
    {
        // Arrange
        var filter = "CPH eq 'ABC123'";
        var orderBy = "UpdatedAtUtc desc";
        var select = "CPH,UpdatedAtUtc";

        _queryServiceMock
            .Setup(x => x.QueryAsync(It.IsAny<QueryParameters>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult
            {
                CollectionName = "sam_cph_holdings",
                Data = new List<Dictionary<string, object?>>(),
                Count = 0,
                TotalCount = 0,
                Skip = 0,
                Top = 100,
                ExecutedAtUtc = DateTime.UtcNow
            });

        // Act
        var result = await _sut.QueryAsync(
            "sam_cph_holdings",
            filter: filter,
            orderBy: orderBy,
            select: select,
            skip: 0,
            top: 100,
            count: true,
            CancellationToken.None);

        // Assert
        result.Filter.Should().Be(filter);
        result.OrderBy.Should().Be(orderBy);
        result.Select.Should().Be(select);
    }

    [Fact]
    public async Task QueryAsync_WithZeroTop_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _sut.QueryAsync(
                "sam_cph_holdings",
                filter: null,
                orderBy: null,
                select: null,
                skip: 0,
                top: 0,
                count: true,
                CancellationToken.None));

        exception.Message.Should().Contain("must be greater than 0");
        exception.ParamName.Should().Be("top");
    }

    [Fact]
    public async Task QueryAsync_WithNegativeTop_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _sut.QueryAsync(
                "sam_cph_holdings",
                filter: null,
                orderBy: null,
                select: null,
                skip: 0,
                top: -5,
                count: true,
                CancellationToken.None));

        exception.Message.Should().Contain("must be greater than 0");
        exception.ParamName.Should().Be("top");
    }
}
