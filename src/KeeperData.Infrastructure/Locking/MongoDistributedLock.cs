using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Locking;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KeeperData.Infrastructure.Locking;

public sealed class MongoDistributedLock : IDistributedLock, IDisposable
{
    private readonly IMongoCollection<DistributedLock> _collection;
    private readonly SemaphoreSlim _initSemaphore = new(1, 1);
    private volatile bool _indexInitialized;

    public MongoDistributedLock(IOptions<MongoConfig> mongoConfig, IMongoClient client)
    {
        var mongoDatabase = client.GetDatabase(mongoConfig.Value.DatabaseName);
        _collection = mongoDatabase.GetCollection<DistributedLock>("distributed_locks");
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (!_indexInitialized)
        {
            await _initSemaphore.WaitAsync(cancellationToken);
            try
            {
                if (!_indexInitialized)
                {
                    var options = new CreateIndexOptions { ExpireAfter = TimeSpan.FromSeconds(0) };
                    var indexDefinition = new IndexKeysDefinitionBuilder<DistributedLock>().Ascending(d => d.ExpiresAtUtc);
                    var indexModel = new CreateIndexModel<DistributedLock>(indexDefinition, options);
                    await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
                    _indexInitialized = true;
                }
            }
            finally
            {
                _initSemaphore.Release();
            }
        }
    }

    public async Task<IDistributedLockHandle?> TryAcquireAsync(string lockName, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(lockName);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(duration, TimeSpan.Zero);

        // Ensure index is created before attempting lock operations
        await InitializeAsync(cancellationToken);

        var ownerId = Guid.NewGuid().ToString();
        var expiresAt = DateTimeOffset.UtcNow.Add(duration);

        var lockDocument = new DistributedLock
        {
            Id = lockName,
            Owner = ownerId,
            ExpiresAtUtc = expiresAt
        };

        try
        {
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
            await _collection.InsertOneAsync(lockDocument, cancellationToken: cancellationToken);
            return new MongoLockHandle(_collection, lockName, ownerId);
        }
        catch (MongoWriteException ex) when (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            // Another process acquired the lock between our check and insert.
            return null;
        }
    }

    public void Dispose() => _initSemaphore?.Dispose();

    private sealed class MongoLockHandle(IMongoCollection<DistributedLock> collection, string lockName, string ownerId) : IDistributedLockHandle
    {
        private bool _disposed;

        /// <summary>
        /// Attempts to renew the lock by extending its expiration time.
        /// </summary>
        /// <param name="extension">The additional time to extend the lock</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>True if the lock was successfully renewed, false if the lock is no longer owned by this handle</returns>
        public async Task<bool> TryRenewAsync(TimeSpan extension, CancellationToken cancellationToken = default)
        {
            if (!_disposed)
            {
                ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(extension, TimeSpan.Zero);

                try
                {
                    var filter = Builders<DistributedLock>.Filter.And(
                        Builders<DistributedLock>.Filter.Eq(d => d.Id, lockName),
                        Builders<DistributedLock>.Filter.Eq(d => d.Owner, ownerId)
                    );

                    var update = Builders<DistributedLock>.Update.Set(d => d.ExpiresAtUtc, DateTimeOffset.UtcNow.Add(extension));

                    var result = await collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken);
                    return result.ModifiedCount > 0;
                }
                catch (MongoException)
                {
                    // Return false on any MongoDB exception during renewal
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                try
                {
                    var filter = Builders<DistributedLock>.Filter.And(
                        Builders<DistributedLock>.Filter.Eq(d => d.Id, lockName),
                        Builders<DistributedLock>.Filter.Eq(d => d.Owner, ownerId)
                    );

                    await collection.DeleteOneAsync(filter);
                }
                catch (MongoException)
                {
                    // Ignore exceptions during cleanup - the TTL index will eventually clean up abandoned locks
                }
                finally
                {
                    _disposed = true;
                }
            }
        }
    }
}