using KeeperData.Data.Attributes;
using KeeperData.Data.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System.Reflection;

namespace KeeperData.Data.Repositories.Implementations;

public class MongoRepository<T> : IGenericRepository<T>
    where T : IEntity
{
    private readonly IMongoCollection<T> _collection;

    public MongoRepository(IOptions<MongoConfig> mongoConfig, IMongoClient client)
    {
        var mongoDatabase = client.GetDatabase(mongoConfig.Value.DatabaseName);
        var collectionName = typeof(T).GetCustomAttribute<CollectionNameAttribute>()?.Name ?? typeof(T).Name;
        _collection = mongoDatabase.GetCollection<T>(collectionName);
    }

    public Task<T> GetByIdAsync(string id, CancellationToken cancellationToken = default) =>
        _collection.Find(x => x.Id == id).FirstOrDefaultAsync(cancellationToken);

    public Task AddAsync(T entity, CancellationToken cancellationToken = default) =>
        _collection.InsertOneAsync(entity, new InsertOneOptions { BypassDocumentValidation = true }, cancellationToken);

    public Task UpdateAsync(T entity, CancellationToken cancellationToken = default) =>
        _collection.ReplaceOneAsync(x => x.Id == entity.Id, entity, cancellationToken: cancellationToken);

    public Task DeleteAsync(string id, CancellationToken cancellationToken = default) =>
        _collection.DeleteOneAsync(x => x.Id == id, cancellationToken);
}