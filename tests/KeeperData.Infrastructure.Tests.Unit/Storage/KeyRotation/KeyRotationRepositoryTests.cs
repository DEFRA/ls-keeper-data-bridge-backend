using FluentAssertions;
using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Storage.KeyRotation;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Storage.KeyRotation;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Storage.KeyRotation;

public class KeyRotationRepositoryTests
{
    private readonly Mock<IMongoCollection<KeyRotationRecord>> _collectionMock = new();
    private readonly Mock<IMongoIndexManager<KeyRotationRecord>> _indexManagerMock = new();
    private readonly Mock<IMongoDatabase> _databaseMock = new();
    private readonly Mock<IMongoClient> _clientMock = new();
    private readonly KeyRotationRepository _sut;

    public KeyRotationRepositoryTests()
    {
        _collectionMock.Setup(c => c.Indexes).Returns(_indexManagerMock.Object);
        _databaseMock.Setup(db => db.GetCollection<KeyRotationRecord>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(_collectionMock.Object);
        _clientMock.Setup(c => c.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>()))
            .Returns(_databaseMock.Object);

        _sut = new KeyRotationRepository(
            Options.Create(new MongoConfig { DatabaseName = "testdb", DatabaseUri = "mongodb://localhost" }),
            _clientMock.Object);
    }

    private static KeyRotationRecord CreateRecord(string id = "rotation-1", KeyRotationStatus status = KeyRotationStatus.Active) => new()
    {
        Id = id,
        BucketName = "cerespfm-dev-dev1-livestockfeeds",
        RotatedAtUtc = DateTime.UtcNow,
        Source = KeyRotationSource.Automatic,
        Status = status,
        FileHash = "abc123",
        KeyIdMasked = "AKI...890"
    };

    private void SetupFind(params KeyRotationRecord[] records)
    {
        var cursorMock = new Mock<IAsyncCursor<KeyRotationRecord>>();
        cursorMock.Setup(c => c.Current).Returns(records);
        cursorMock.SetupSequence(c => c.MoveNext(It.IsAny<CancellationToken>()))
            .Returns(records.Length > 0).Returns(false);
        cursorMock.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(records.Length > 0).ReturnsAsync(false);

        _collectionMock.Setup(c => c.FindAsync(
                It.IsAny<FilterDefinition<KeyRotationRecord>>(),
                It.IsAny<FindOptions<KeyRotationRecord, KeyRotationRecord>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(cursorMock.Object);
    }

    [Fact]
    public void Constructor_UsesCollectionNameFromAttribute()
    {
        // Assert
        _databaseMock.Verify(db => db.GetCollection<KeyRotationRecord>(
            "external_storage_key_rotations", It.IsAny<MongoCollectionSettings>()), Times.Once);
    }

    [Fact]
    public async Task GetActiveAsync_ReturnsActiveRecord()
    {
        // Arrange
        var record = CreateRecord();
        SetupFind(record);

        // Act
        var result = await _sut.GetActiveAsync();

        // Assert
        result.Should().BeSameAs(record);
    }

    [Fact]
    public async Task GetActiveAsync_WithNoActiveRecord_ReturnsNull()
    {
        // Arrange
        SetupFind();

        // Act
        var result = await _sut.GetActiveAsync();

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task GetByIdAsync_ReturnsRecord()
    {
        // Arrange
        var record = CreateRecord("some-id");
        SetupFind(record);

        // Act
        var result = await _sut.GetByIdAsync("some-id");

        // Assert
        result.Should().BeSameAs(record);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public async Task GetByIdAsync_WithBlankId_Throws(string? id)
    {
        // Act
        var act = () => _sut.GetByIdAsync(id!);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task GetLatestObservedFileHashAsync_ReturnsHashOfLatestRecord()
    {
        // Arrange
        SetupFind(CreateRecord());

        // Act
        var result = await _sut.GetLatestObservedFileHashAsync();

        // Assert
        result.Should().Be("abc123");
    }

    [Fact]
    public async Task GetLatestObservedFileHashAsync_WithNoRecords_ReturnsNull()
    {
        // Arrange
        SetupFind();

        // Act
        var result = await _sut.GetLatestObservedFileHashAsync();

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task ActivateAsync_SupersedesCurrentActiveThenInsertsNewActiveRecord()
    {
        // Arrange
        var record = CreateRecord(status: KeyRotationStatus.Failed);

        // Act
        await _sut.ActivateAsync(record);

        // Assert
        record.Status.Should().Be(KeyRotationStatus.Active);
        _collectionMock.Verify(c => c.UpdateManyAsync(
            It.IsAny<FilterDefinition<KeyRotationRecord>>(),
            It.IsAny<UpdateDefinition<KeyRotationRecord>>(),
            It.IsAny<UpdateOptions>(),
            It.IsAny<CancellationToken>()), Times.Once);
        _collectionMock.Verify(c => c.InsertOneAsync(
            record, It.IsAny<InsertOneOptions>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ActivateAsync_CreatesIndexesOnlyOnce()
    {
        // Act
        await _sut.ActivateAsync(CreateRecord("a"));
        await _sut.ActivateAsync(CreateRecord("b"));

        // Assert
        _indexManagerMock.Verify(im => im.CreateManyAsync(
            It.IsAny<IEnumerable<CreateIndexModel<KeyRotationRecord>>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ActivateAsync_WithNullRecord_Throws()
    {
        // Act
        var act = () => _sut.ActivateAsync(null!);

        // Assert
        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task AddFailedAsync_ForcesFailedStatusAndInsertsWithoutSuperseding()
    {
        // Arrange
        var record = CreateRecord(status: KeyRotationStatus.Active);

        // Act
        await _sut.AddFailedAsync(record);

        // Assert
        record.Status.Should().Be(KeyRotationStatus.Failed);
        _collectionMock.Verify(c => c.InsertOneAsync(
            record, It.IsAny<InsertOneOptions>(), It.IsAny<CancellationToken>()), Times.Once);
        _collectionMock.Verify(c => c.UpdateManyAsync(
            It.IsAny<FilterDefinition<KeyRotationRecord>>(),
            It.IsAny<UpdateDefinition<KeyRotationRecord>>(),
            It.IsAny<UpdateOptions>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task AddFailedAsync_WithNullRecord_Throws()
    {
        // Act
        var act = () => _sut.AddFailedAsync(null!);

        // Assert
        await act.Should().ThrowAsync<ArgumentNullException>();
    }

    [Fact]
    public async Task GetSuccessfulPageAsync_ReturnsItemsAndTotalCount()
    {
        // Arrange
        SetupFind(CreateRecord("a"), CreateRecord("b", KeyRotationStatus.Superseded));
        _collectionMock.Setup(c => c.CountDocumentsAsync(
                It.IsAny<FilterDefinition<KeyRotationRecord>>(),
                It.IsAny<CountOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(12);

        // Act
        var page = await _sut.GetSuccessfulPageAsync(1, 10);

        // Assert
        page.TotalCount.Should().Be(12);
        page.Items.Should().HaveCount(2);
    }

    [Theory]
    [InlineData(0, 10)]
    [InlineData(1, 0)]
    public async Task GetSuccessfulPageAsync_WithInvalidPaging_Throws(int page, int pageSize)
    {
        // Act
        var act = () => _sut.GetSuccessfulPageAsync(page, pageSize);

        // Assert
        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
    }
}
