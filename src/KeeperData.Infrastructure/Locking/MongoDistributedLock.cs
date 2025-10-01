using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Locking;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KeeperData.Infrastructure.Locking;

public class MongoDistributedLock : IDistributedLock
{
    private readonly IMongoCollection<DistributedLock> _collection;
    private static bool _ttlIndexEnsured;
    private static readonly object _lock = new();

    public MongoDistributedLock(IOptions<MongoConfig> mongoConfig, IMongoClient client)
    {
        var mongoDatabase = client.GetDatabase(mongoConfig.Value.DatabaseName);
        _collection = mongoDatabase.GetCollection<DistributedLock>("distributed_locks");
        EnsureTtlIndexExists();
    }


    // TTL index for cleanup of abandoned locks.
    private void EnsureTtlIndexExists()
    {
        if (_ttlIndexEnsured)
        {
            return;
        }

        lock (_lock)
        {
            if (_ttlIndexEnsured)
            {
                return;
            }

            // set ExpireAfter to 0. This tells MongoDB to look at ExpiresAtUtc and expire the document at that specific time.
            var options = new CreateIndexOptions { ExpireAfter = TimeSpan.FromSeconds(0) };
            var indexDefinition = new IndexKeysDefinitionBuilder<DistributedLock>().Ascending(d => d.ExpiresAtUtc);
            var indexModel = new CreateIndexModel<DistributedLock>(indexDefinition, options);

            _collection.Indexes.CreateOne(indexModel);

            _ttlIndexEnsured = true;
        }
    }

    public async Task<IDisposable?> TryAcquireAsync(string lockName, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        var ownerId = Guid.NewGuid().ToString();
        var expiresAt = DateTimeOffset.UtcNow.Add(duration);

        var lockDocument = new DistributedLock
        {
            Id = lockName,
            Owner = ownerId,
            ExpiresAtUtc = expiresAt
        };

        // Atomically find and replace an expired lock.
        var filter = Builders<DistributedLock>.Filter.And(
            Builders<DistributedLock>.Filter.Eq(d => d.Id, lockName),
            Builders<DistributedLock>.Filter.Lt(d => d.ExpiresAtUtc, DateTimeOffset.UtcNow)
        );

        var replaced = await _collection.FindOneAndReplaceAsync(filter, lockDocument, cancellationToken: cancellationToken);

        if (replaced != null)
        {
            // Successfully replaced an expired lock
            return new MongoLockHandle(_collection, lockName, ownerId);
        }

        // If no lock was replaced, it's either unexpired or doesn't exist.
        // this will fail if an unexpired lock already exists.
        try
        {
            await _collection.InsertOneAsync(lockDocument, cancellationToken: cancellationToken);

            return new MongoLockHandle(_collection, lockName, ownerId);
        }
        catch (MongoWriteException ex) when (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            // Another process acquired the lock between our check and insert.
            return null;
        }
    }

    private sealed class MongoLockHandle : IDisposable
    {
        private readonly IMongoCollection<DistributedLock> _collection;
        private readonly string _lockName;
        private readonly string _ownerId;

        public MongoLockHandle(IMongoCollection<DistributedLock> collection, string lockName, string ownerId)
        {
            _collection = collection;
            _lockName = lockName;
            _ownerId = ownerId;
        }

        public void Dispose()
        {
            // Only release the lock if we are still the owner.
            var filter = Builders<DistributedLock>.Filter.And(
                Builders<DistributedLock>.Filter.Eq(d => d.Id, _lockName),
                Builders<DistributedLock>.Filter.Eq(d => d.Owner, _ownerId)
            );
            _collection.DeleteOne(filter);
        }
    }
}