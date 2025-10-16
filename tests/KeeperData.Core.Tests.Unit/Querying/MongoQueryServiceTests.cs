using FluentAssertions;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Impl;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using System.Collections.Immutable;

namespace KeeperData.Core.Tests.Unit.Querying;

public class MongoQueryServiceTests
{
    private readonly Mock<IMongoClient> _mongoClientMock;
    private readonly Mock<IMongoDatabase> _mongoDatabaseMock;
    private readonly Mock<IMongoCollection<BsonDocument>> _collectionMock;
    private readonly Mock<IOptions<IDatabaseConfig>> _databaseConfigMock;
    private readonly Mock<IDataSetDefinitions> _dataSetDefinitionsMock;
    private readonly Mock<ILogger<MongoQueryService>> _loggerMock;
    private readonly MongoQueryService _sut;

    public MongoQueryServiceTests()
    {
        _mongoClientMock = new Mock<IMongoClient>();
        _mongoDatabaseMock = new Mock<IMongoDatabase>();
        _collectionMock = new Mock<IMongoCollection<BsonDocument>>();
        _databaseConfigMock = new Mock<IOptions<IDatabaseConfig>>();
        _dataSetDefinitionsMock = new Mock<IDataSetDefinitions>();
        _loggerMock = new Mock<ILogger<MongoQueryService>>();

        var databaseConfig = new Mock<IDatabaseConfig>();
        databaseConfig.Setup(x => x.DatabaseName).Returns("TestDatabase");
        _databaseConfigMock.Setup(x => x.Value).Returns(databaseConfig.Object);

        var definitions = new[]
        {
            new DataSetDefinition("sam_cph_holdings", "LITP_SAMCPHHOLDING_{0}", "yyyyMMdd", "CPH", "CHANGETYPE")
        };
        _dataSetDefinitionsMock.Setup(x => x.All).Returns(definitions.ToImmutableArray());

        _mongoClientMock.Setup(x => x.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>()))
            .Returns(_mongoDatabaseMock.Object);

        _mongoDatabaseMock.Setup(x => x.GetCollection<BsonDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(_collectionMock.Object);

        _sut = new MongoQueryService(
            _mongoClientMock.Object,
            _databaseConfigMock.Object,
            _dataSetDefinitionsMock.Object,
            _loggerMock.Object);
    }

    [Fact]
    public async Task QueryAsync_WithValidCollectionName_ReturnsResults()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument
            {
                { "_id", "ABC123" },
                { "CPH", "ABC123" },
                { "FieldName", "Value1" },
                { "IsDeleted", false }
            },
            new BsonDocument
            {
                { "_id", "XYZ789" },
                { "CPH", "XYZ789" },
                { "FieldName", "Value2" },
                { "IsDeleted", false }
            }
        };

        var cursorMock = new Mock<IAsyncCursor<BsonDocument>>();
        cursorMock.Setup(x => x.Current).Returns(documents);
        cursorMock.SetupSequence(x => x.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);

        _collectionMock.Setup(x => x.FindAsync(
            It.IsAny<FilterDefinition<BsonDocument>>(),
            It.IsAny<FindOptions<BsonDocument>>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursorMock.Object);

        _collectionMock.Setup(x => x.CountDocumentsAsync(
            It.IsAny<FilterDefinition<BsonDocument>>(),
            It.IsAny<CountOptions>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(2);

        // Act
        var result = await _sut.QueryAsync(
            "sam_cph_holdings",
            filter: null,
            orderBy: null,
            skip: 0,
            top: 10,
            count: true,
            CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.CollectionName.Should().Be("sam_cph_holdings");
        result.Data.Should().HaveCount(2);
        result.Count.Should().Be(2);
        result.TotalCount.Should().Be(2);
        result.Data[0]["CPH"].Should().Be("ABC123");
        result.Data[1]["CPH"].Should().Be("XYZ789");
    }

    [Fact]
    public async Task QueryAsync_WithInvalidCollectionName_ThrowsArgumentException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _sut.QueryAsync(
                "invalid_collection",
                filter: null,
                orderBy: null,
                skip: 0,
                top: 10,
                count: true,
                CancellationToken.None));
    }

    [Fact]
    public async Task QueryAsync_WithEmptyCollectionName_ThrowsArgumentException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _sut.QueryAsync(
                "",
                filter: null,
                orderBy: null,
                skip: 0,
                top: 10,
                count: true,
                CancellationToken.None));
    }

    [Fact]
    public async Task QueryAsync_WithPagination_ReturnsCorrectPageInfo()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument { { "_id", "1" }, { "CPH", "CPH1" } }
        };

        var cursorMock = new Mock<IAsyncCursor<BsonDocument>>();
        cursorMock.Setup(x => x.Current).Returns(documents);
        cursorMock.SetupSequence(x => x.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);

        _collectionMock.Setup(x => x.FindAsync(
            It.IsAny<FilterDefinition<BsonDocument>>(),
            It.IsAny<FindOptions<BsonDocument>>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursorMock.Object);

        _collectionMock.Setup(x => x.CountDocumentsAsync(
            It.IsAny<FilterDefinition<BsonDocument>>(),
            It.IsAny<CountOptions>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(100);

        // Act
        var result = await _sut.QueryAsync(
            "sam_cph_holdings",
            filter: null,
            orderBy: null,
            skip: 20,
            top: 10,
            count: true,
            CancellationToken.None);

        // Assert
        result.Skip.Should().Be(20);
        result.Top.Should().Be(10);
        result.TotalCount.Should().Be(100);
    }

    [Fact]
    public async Task QueryAsync_WithTopGreaterThanMax_UsesMaxPageSize()
    {
        // Arrange
        var documents = new List<BsonDocument>();
        var cursorMock = new Mock<IAsyncCursor<BsonDocument>>();
        cursorMock.Setup(x => x.Current).Returns(documents);
        cursorMock.SetupSequence(x => x.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);

        _collectionMock.Setup(x => x.FindAsync(
            It.IsAny<FilterDefinition<BsonDocument>>(),
            It.IsAny<FindOptions<BsonDocument>>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursorMock.Object);

        _collectionMock.Setup(x => x.CountDocumentsAsync(
            It.IsAny<FilterDefinition<BsonDocument>>(),
            It.IsAny<CountOptions>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(0);

        // Act
        var result = await _sut.QueryAsync(
            "sam_cph_holdings",
            filter: null,
            orderBy: null,
            skip: 0,
            top: 5000, // Greater than max of 1000
            count: true,
            CancellationToken.None);

        // Assert
        result.Top.Should().Be(1000); // Should be capped at max
    }

    [Fact]
    public async Task QueryAsync_WithCountFalse_DoesNotIncludeTotalCount()
    {
        // Arrange
        var documents = new List<BsonDocument>
        {
            new BsonDocument { { "_id", "1" }, { "CPH", "CPH1" } }
        };

        var cursorMock = new Mock<IAsyncCursor<BsonDocument>>();
        cursorMock.Setup(x => x.Current).Returns(documents);
        cursorMock.SetupSequence(x => x.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);

        _collectionMock.Setup(x => x.FindAsync(
            It.IsAny<FilterDefinition<BsonDocument>>(),
            It.IsAny<FindOptions<BsonDocument>>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursorMock.Object);

        // Act
        var result = await _sut.QueryAsync(
            "sam_cph_holdings",
            filter: null,
            orderBy: null,
            skip: 0,
            top: 10,
            count: false,
            CancellationToken.None);

        // Assert
        result.TotalCount.Should().BeNull();
        _collectionMock.Verify(
            x => x.CountDocumentsAsync(
                It.IsAny<FilterDefinition<BsonDocument>>(),
                It.IsAny<CountOptions>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }
}