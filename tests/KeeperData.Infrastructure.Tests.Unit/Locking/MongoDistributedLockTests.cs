using FluentAssertions;
using KeeperData.Core.Domain.Entities;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Locking;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
using Moq;
using System.Net;
using System.Reflection;

namespace KeeperData.Infrastructure.Tests.Unit.Locking;

public class MongoDistributedLockTests
{
    private readonly Mock<IMongoCollection<DistributedLock>> _collectionMock;
    private readonly Mock<IMongoIndexManager<DistributedLock>> _indexManagerMock;
    private readonly IOptions<MongoConfig> _mongoConfig;
    private readonly Mock<IMongoClient> _mongoClientMock;

    public MongoDistributedLockTests()
    {
        _mongoConfig = Options.Create(new MongoConfig { DatabaseName = "testdb" });
        _mongoClientMock = new Mock<IMongoClient>();
        var mongoDatabaseMock = new Mock<IMongoDatabase>();
        _collectionMock = new Mock<IMongoCollection<DistributedLock>>();
        _indexManagerMock = new Mock<IMongoIndexManager<DistributedLock>>();

        _collectionMock.Setup(c => c.Indexes).Returns(_indexManagerMock.Object);
        _collectionMock.Setup(c => c.Database).Returns(mongoDatabaseMock.Object);
        _collectionMock.Setup(c => c.CollectionNamespace).Returns(new CollectionNamespace("testdb", "distributed_locks"));

        mongoDatabaseMock.Setup(db => db.DatabaseNamespace).Returns(new DatabaseNamespace("testdb"));
        mongoDatabaseMock.Setup(db => db.GetCollection<DistributedLock>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(_collectionMock.Object);

        _mongoClientMock.Setup(client => client.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>()))
            .Returns(mongoDatabaseMock.Object);
    }

    [Fact]
    public async Task InitializeAsync_WhenCalled_CreatesTtlIndex()
    {
        // Arrange
        CreateIndexModel<DistributedLock>? capturedIndexModel = null;
        _indexManagerMock.Setup(im => im.CreateOneAsync(It.IsAny<CreateIndexModel<DistributedLock>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()))
            .Callback<CreateIndexModel<DistributedLock>, CreateOneIndexOptions, CancellationToken>((model, options, token) => capturedIndexModel = model)
            .ReturnsAsync("test-index");

        var sut = new MongoDistributedLock(_mongoConfig, _mongoClientMock.Object);

        // Act
        await sut.InitializeAsync();

        // Assert
        _indexManagerMock.Verify(im => im.CreateOneAsync(It.IsAny<CreateIndexModel<DistributedLock>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()), Times.Once);
        capturedIndexModel.Should().NotBeNull();
        capturedIndexModel!.Options.ExpireAfter.Should().Be(TimeSpan.Zero);
        var renderedKeys = capturedIndexModel.Keys.Render(BsonSerializer.SerializerRegistry.GetSerializer<DistributedLock>(), BsonSerializer.SerializerRegistry);
        renderedKeys.ToString().Should().Be("{ \"ExpiresAtUtc\" : 1 }");
    }

    [Fact]
    public async Task InitializeAsync_WhenCalledMultipleTimes_CreatesIndexOnlyOnce()
    {
        // Arrange
        _indexManagerMock.Setup(im => im.CreateOneAsync(It.IsAny<CreateIndexModel<DistributedLock>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("test-index");

        var sut = new MongoDistributedLock(_mongoConfig, _mongoClientMock.Object);

        // Act
        await sut.InitializeAsync();
        await sut.InitializeAsync();

        // Assert
        _indexManagerMock.Verify(im => im.CreateOneAsync(It.IsAny<CreateIndexModel<DistributedLock>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TryAcquireAsync_WhenNoLockExists_AcquiresLockSuccessfully()
    {
        // Arrange
        _indexManagerMock.Setup(im => im.CreateOneAsync(It.IsAny<CreateIndexModel<DistributedLock>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("test-index");

        _collectionMock.Setup(c => c.FindOneAndReplaceAsync(
                It.IsAny<FilterDefinition<DistributedLock>>(),
                It.IsAny<DistributedLock>(),
                It.IsAny<FindOneAndReplaceOptions<DistributedLock, DistributedLock>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((DistributedLock)null!);

        _collectionMock.Setup(c => c.InsertOneAsync(
                It.IsAny<DistributedLock>(),
                It.IsAny<InsertOneOptions>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var sut = new MongoDistributedLock(_mongoConfig, _mongoClientMock.Object);

        // Act
        var lockHandle = await sut.TryAcquireAsync("test-lock", TimeSpan.FromMinutes(1));

        // Assert
        lockHandle.Should().NotBeNull();
        _collectionMock.Verify(c => c.InsertOneAsync(It.IsAny<DistributedLock>(), null, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TryAcquireAsync_WhenLockIsExpired_ReplacesAndAcquiresLockSuccessfully()
    {
        // Arrange
        _indexManagerMock.Setup(im => im.CreateOneAsync(It.IsAny<CreateIndexModel<DistributedLock>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("test-index");

        var expiredLock = new DistributedLock { Id = "test-lock", Owner = "old-owner", ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(-5) };
        _collectionMock.Setup(c => c.FindOneAndReplaceAsync(
                It.IsAny<FilterDefinition<DistributedLock>>(),
                It.IsAny<DistributedLock>(),
                It.IsAny<FindOneAndReplaceOptions<DistributedLock, DistributedLock>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(expiredLock);

        var sut = new MongoDistributedLock(_mongoConfig, _mongoClientMock.Object);

        // Act
        var lockHandle = await sut.TryAcquireAsync("test-lock", TimeSpan.FromMinutes(1));

        // Assert
        lockHandle.Should().NotBeNull();
        _collectionMock.Verify(c => c.InsertOneAsync(It.IsAny<DistributedLock>(), null, It.IsAny<CancellationToken>()), Times.Never);
        _collectionMock.Verify(c => c.FindOneAndReplaceAsync(It.IsAny<FilterDefinition<DistributedLock>>(), It.IsAny<DistributedLock>(), It.IsAny<FindOneAndReplaceOptions<DistributedLock, DistributedLock>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TryAcquireAsync_WhenLockIsHeld_FailsToAcquireLock()
    {
        // Arrange
        _indexManagerMock.Setup(im => im.CreateOneAsync(It.IsAny<CreateIndexModel<DistributedLock>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("test-index");

        _collectionMock.Setup(c => c.FindOneAndReplaceAsync(
                It.IsAny<FilterDefinition<DistributedLock>>(),
                It.IsAny<DistributedLock>(),
                It.IsAny<FindOneAndReplaceOptions<DistributedLock, DistributedLock>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((DistributedLock)null!);

        var mongoWriteException = CreateMongoWriteExceptionForDuplicateKey();

        _collectionMock.Setup(c => c.InsertOneAsync(
                It.IsAny<DistributedLock>(),
                It.IsAny<InsertOneOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(mongoWriteException);

        var sut = new MongoDistributedLock(_mongoConfig, _mongoClientMock.Object);

        // Act
        var lockHandle = await sut.TryAcquireAsync("test-lock", TimeSpan.FromMinutes(1));

        // Assert
        lockHandle.Should().BeNull();
    }

    [Fact]
    public async Task DisposeAsync_WhenCalledOnAcquiredLock_DeletesTheLock()
    {
        // Arrange
        _indexManagerMock.Setup(im => im.CreateOneAsync(It.IsAny<CreateIndexModel<DistributedLock>>(), It.IsAny<CreateOneIndexOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("test-index");

        _collectionMock.Setup(c => c.FindOneAndReplaceAsync(
                It.IsAny<FilterDefinition<DistributedLock>>(),
                It.IsAny<DistributedLock>(),
                It.IsAny<FindOneAndReplaceOptions<DistributedLock, DistributedLock>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync((DistributedLock)null!);

        _collectionMock.Setup(c => c.InsertOneAsync(
                It.IsAny<DistributedLock>(),
                It.IsAny<InsertOneOptions>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        _collectionMock.Setup(c => c.DeleteOneAsync(
            It.IsAny<FilterDefinition<DistributedLock>>(),
            It.IsAny<DeleteOptions>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteResult.Acknowledged(1));

        var sut = new MongoDistributedLock(_mongoConfig, _mongoClientMock.Object);

        // Act
        var lockHandle = await sut.TryAcquireAsync("test-lock", TimeSpan.FromMinutes(1));
        lockHandle.Should().NotBeNull();
        await lockHandle!.DisposeAsync();

        // Assert
        _collectionMock.Verify(c => c.DeleteOneAsync(It.IsAny<FilterDefinition<DistributedLock>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    private static MongoWriteException CreateMongoWriteExceptionForDuplicateKey()
    {
        var writeErrorConstructor = typeof(WriteError).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            null,
            [typeof(ServerErrorCategory), typeof(int), typeof(string), typeof(BsonDocument)],
            null);

        var writeError = (WriteError)writeErrorConstructor!.Invoke(
        [
            ServerErrorCategory.DuplicateKey,
            11000, // Standard code for duplicate key
            "E11000 duplicate key error collection",
            new BsonDocument()
        ]);

        var connectionId = new ConnectionId(new ServerId(new ClusterId(1), new DnsEndPoint("localhost", 27017)), 1);
        return new MongoWriteException(connectionId, writeError, null, null);
    }
}