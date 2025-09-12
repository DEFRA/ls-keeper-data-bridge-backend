using KeeperData.Core.Attributes;
using KeeperData.Core.Domain;
using KeeperData.Core.Repositories;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System.Reflection;

namespace KeeperData.Infrastructure.Database.Repositories;

public class GenericRepository<T> : IGenericRepository<T>
    where T : IEntity
{
    private readonly IMongoCollection<T> _collection;

    public GenericRepository(IOptions<MongoConfig> mongoConfig, IMongoClient client)
    {
        var mongoDatabase = client.GetDatabase(mongoConfig.Value.DatabaseName);
        var collectionName = typeof(T).GetCustomAttribute<CollectionNameAttribute>()?.Name ?? typeof(T).Name;
        _collection = mongoDatabase.GetCollection<T>(collectionName);
    }

    public async Task<T> GetByIdAsync(string id, CancellationToken cancellationToken = default)
    {
        var filter = Builders<T>.Filter.Eq(x => x.Id, id);
        var cursor = await _collection.FindAsync(filter, cancellationToken: cancellationToken);
        return await cursor.FirstOrDefaultAsync(cancellationToken);
    }

    public Task AddAsync(T entity, CancellationToken cancellationToken = default) =>
        _collection.InsertOneAsync(entity, new InsertOneOptions { BypassDocumentValidation = true }, cancellationToken);

    public Task UpdateAsync(T entity, CancellationToken cancellationToken = default) =>
        _collection.ReplaceOneAsync(x => x.Id == entity.Id, entity, cancellationToken: cancellationToken);

    public Task BulkUpsertAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        var models = entities.Select(entity =>
            new ReplaceOneModel<T>(
                filter: Builders<T>.Filter.Eq(x => x.Id, entity.Id),
                replacement: entity)
            {
                IsUpsert = true
            });

        return _collection.BulkWriteAsync(models.ToList(), cancellationToken: cancellationToken);
    }

    public Task BulkUpsertWithCustomFilterAsync(IEnumerable<(FilterDefinition<T> Filter, T Entity)> items, CancellationToken cancellationToken = default)
    {
        var models = items.Select(item =>
            new ReplaceOneModel<T>(
                filter: item.Filter,
                replacement: item.Entity)
            {
                IsUpsert = true
            });

        return _collection.BulkWriteAsync(models.ToList(), cancellationToken: cancellationToken);
    }

    public Task DeleteAsync(string id, CancellationToken cancellationToken = default) =>
        _collection.DeleteOneAsync(x => x.Id == id, cancellationToken);
}