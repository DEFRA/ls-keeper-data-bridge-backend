using FluentAssertions;
using KeeperData.Core.Attributes;
using KeeperData.Core.Domain;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Database.Repositories;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Moq;
using System.Reflection;

namespace KeeperData.Infrastructure.Tests.Unit.Database.Repositories;

public class GenericRepositoryTests
{
    private readonly IOptions<MongoConfig> _mongoConfig;
    private readonly Mock<IMongoClient> _mongoClientMock = new();
    private readonly Mock<IMongoDatabase> _mongoDatabaseMock = new();
    private readonly Mock<IAsyncCursor<TestEntity>> _asyncCursorMock = new();
    private readonly Mock<IMongoCollection<TestEntity>> _mongoCollectionMock = new();

    private readonly GenericRepository<TestEntity> _sut;

    public GenericRepositoryTests()
    {
        _mongoConfig = Options.Create(new MongoConfig { DatabaseName = "DatabaseName" });

        _mongoDatabaseMock
            .Setup(db => db.GetCollection<TestEntity>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
            .Returns(_mongoCollectionMock.Object);

        _mongoClientMock
            .Setup(client => client.GetDatabase(It.IsAny<string>(), It.IsAny<MongoDatabaseSettings>()))
            .Returns(_mongoDatabaseMock.Object);

        _asyncCursorMock
            .SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);

        _mongoCollectionMock
            .Setup(c => c.FindAsync(
                It.IsAny<FilterDefinition<TestEntity>>(),
                It.IsAny<FindOptions<TestEntity, TestEntity>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(_asyncCursorMock.Object);

        _sut = new GenericRepository<TestEntity>(_mongoConfig, _mongoClientMock.Object);

        typeof(GenericRepository<TestEntity>)
            .GetField("_collection", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(_sut, _mongoCollectionMock.Object);
    }

    [Fact]
    public async Task GivenValidId_WhenCallingGetByIdAsync_ThenReturnsExpectedEntity()
    {
        var expected = new TestEntity { Id = Guid.NewGuid().ToString(), Name = "Test Entity" };

        _asyncCursorMock
            .SetupGet(c => c.Current)
            .Returns([expected]);

        var result = await _sut.GetByIdAsync(expected.Id, CancellationToken.None);

        result.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task GivenEntity_WhenCallingAddAsync_ThenInsertOneIsCalled()
    {
        var entity = new TestEntity { Id = Guid.NewGuid().ToString(), Name = "New Entity" };

        _mongoCollectionMock
            .Setup(c => c.InsertOneAsync(entity, It.IsAny<InsertOneOptions>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable();

        await _sut.AddAsync(entity, CancellationToken.None);

        _mongoCollectionMock.Verify();
    }

    [Fact]
    public async Task GivenEntity_WhenCallingUpdateAsync_ThenReplaceOneIsCalled()
    {
        var entity = new TestEntity { Id = Guid.NewGuid().ToString(), Name = "Updated Entity" };

        var replaceResultMock = new Mock<ReplaceOneResult>();
        replaceResultMock.SetupAllProperties();

        _mongoCollectionMock
            .Setup(c => c.ReplaceOneAsync(
                It.IsAny<FilterDefinition<TestEntity>>(),
                entity,
                It.IsAny<ReplaceOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(replaceResultMock.Object)
            .Verifiable();

        await _sut.UpdateAsync(entity, CancellationToken.None);

        _mongoCollectionMock.Verify();
    }

    [Fact]
    public async Task GivenEntities_WhenCallingBulkUpsertAsync_ThenBulkWriteIsCalledWithUpsert()
    {
        var entities = new[]
        {
            new TestEntity { Id = Guid.NewGuid().ToString(), Name = "Test Entity 1" },
            new TestEntity { Id = Guid.NewGuid().ToString(), Name = "Test Entity 2" }
        };

        IEnumerable<WriteModel<TestEntity>>? capturedModels = null;
        _mongoCollectionMock.Setup(c => c.BulkWriteAsync(It.IsAny<IEnumerable<WriteModel<TestEntity>>>(), null, It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<WriteModel<TestEntity>>, BulkWriteOptions?, CancellationToken>((models, _, _) =>
            {
                capturedModels = models;
            })
            .ReturnsAsync((BulkWriteResult<TestEntity>?)null);

        await _sut.BulkUpsertAsync(entities, CancellationToken.None);

        _mongoCollectionMock.Verify(c => c.BulkWriteAsync(It.IsAny<IEnumerable<WriteModel<TestEntity>>>(), null, It.IsAny<CancellationToken>()),
            Times.Once);

        capturedModels.Should().NotBeNull().And.HaveCount(2);
        capturedModels!.All(m => m is ReplaceOneModel<TestEntity> model
            && model.IsUpsert
            && entities.Any(e => e.Id == model.Replacement.Id)).Should().BeTrue();
    }

    [Fact]
    public async Task GivenFilteredEntities_WhenCallingBulkUpsertWithCustomFilterAsync_ThenBulkWriteIsCalledWithUpsert()
    {
        var items = new (FilterDefinition<TestEntity> Filter, TestEntity Entity)[]
        {
            (Builders<TestEntity>.Filter.Eq(x => x.Name, "One"), new TestEntity { Id = "1", Name = "One" }),
            (Builders<TestEntity>.Filter.Eq(x => x.Name, "Two"), new TestEntity { Id = "2", Name = "Two" })
        };

        IEnumerable<WriteModel<TestEntity>>? capturedModels = null;
        _mongoCollectionMock.Setup(c => c.BulkWriteAsync(It.IsAny<IEnumerable<WriteModel<TestEntity>>>(), null, It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<WriteModel<TestEntity>>, BulkWriteOptions?, CancellationToken>((models, _, _) =>
            {
                capturedModels = models;
            })
            .ReturnsAsync((BulkWriteResult<TestEntity>?)null);

        await _sut.BulkUpsertWithCustomFilterAsync(items, CancellationToken.None);

        _mongoCollectionMock.Verify(c => c.BulkWriteAsync(It.IsAny<IEnumerable<WriteModel<TestEntity>>>(), null, It.IsAny<CancellationToken>()),
            Times.Once);

        capturedModels.Should().NotBeNull().And.HaveCount(2);
        capturedModels!.All(m =>
        {
            return m is ReplaceOneModel<TestEntity> r
                && r.IsUpsert
                && items.Any(i => i.Entity.Id == r.Replacement.Id
                    && i.Entity.Name == r.Replacement.Name);
        }).Should().BeTrue();
    }

    [Fact]
    public async Task GivenValidId_WhenCallingDeleteAsync_ThenDeleteOneIsCalled()
    {
        var id = Guid.NewGuid().ToString();

        _mongoCollectionMock
            .Setup(c => c.DeleteOneAsync(
                It.IsAny<FilterDefinition<TestEntity>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<DeleteResult>())
            .Verifiable();

        await _sut.DeleteAsync(id, CancellationToken.None);

        _mongoCollectionMock.Verify();
    }
}

[CollectionName("TestEntities")]
public class TestEntity : IEntity
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
}