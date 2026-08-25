using KeeperData.Core.Attributes;
using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Storage.KeyRotation;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System.Reflection;

namespace KeeperData.Infrastructure.Storage.KeyRotation;

/// <summary>
/// Mongo-backed append-only store for <see cref="KeyRotationRecord"/> history.
/// </summary>
public sealed class KeyRotationRepository : IKeyRotationRepository, IDisposable
{
    private static readonly KeyRotationStatus[] SuccessfulStatuses = [KeyRotationStatus.Active, KeyRotationStatus.Superseded];

    private readonly IMongoCollection<KeyRotationRecord> _collection;
    private readonly SemaphoreSlim _initSemaphore = new(1, 1);
    private volatile bool _indexesInitialized;

    public KeyRotationRepository(IOptions<MongoConfig> mongoConfig, IMongoClient client)
    {
        var mongoDatabase = client.GetDatabase(mongoConfig.Value.DatabaseName);
        var collectionName = typeof(KeyRotationRecord).GetCustomAttribute<CollectionNameAttribute>()?.Name
            ?? nameof(KeyRotationRecord);
        _collection = mongoDatabase.GetCollection<KeyRotationRecord>(collectionName);
    }

    public async Task<KeyRotationRecord?> GetActiveAsync(CancellationToken cancellationToken = default)
    {
        var filter = Builders<KeyRotationRecord>.Filter.Eq(x => x.Status, KeyRotationStatus.Active);
        return await _collection.Find(filter)
            .SortByDescending(x => x.RotatedAtUtc)
            .FirstOrDefaultAsync(cancellationToken);
    }

    public async Task<KeyRotationRecord?> GetByIdAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        var filter = Builders<KeyRotationRecord>.Filter.Eq(x => x.Id, id);
        return await _collection.Find(filter).FirstOrDefaultAsync(cancellationToken);
    }

    public async Task<string?> GetLatestObservedFileHashAsync(CancellationToken cancellationToken = default)
    {
        var filter = Builders<KeyRotationRecord>.Filter.Ne(x => x.FileHash, null);
        var latest = await _collection.Find(filter)
            .SortByDescending(x => x.RotatedAtUtc)
            .Limit(1)
            .FirstOrDefaultAsync(cancellationToken);

        return latest?.FileHash;
    }

    public async Task ActivateAsync(KeyRotationRecord record, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);

        await EnsureIndexesAsync(cancellationToken);

        var activeFilter = Builders<KeyRotationRecord>.Filter.Eq(x => x.Status, KeyRotationStatus.Active);
        var supersede = Builders<KeyRotationRecord>.Update.Set(x => x.Status, KeyRotationStatus.Superseded);
        await _collection.UpdateManyAsync(activeFilter, supersede, cancellationToken: cancellationToken);

        record.Status = KeyRotationStatus.Active;
        await _collection.InsertOneAsync(record, new InsertOneOptions(), cancellationToken);
    }

    public async Task AddFailedAsync(KeyRotationRecord record, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);

        await EnsureIndexesAsync(cancellationToken);

        record.Status = KeyRotationStatus.Failed;
        await _collection.InsertOneAsync(record, new InsertOneOptions(), cancellationToken);
    }

    public async Task<KeyRotationPage> GetSuccessfulPageAsync(int page, int pageSize, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(pageSize, 1);

        var filter = Builders<KeyRotationRecord>.Filter.In(x => x.Status, SuccessfulStatuses);

        var totalCount = await _collection.CountDocumentsAsync(filter, cancellationToken: cancellationToken);

        var items = await _collection.Find(filter)
            .SortByDescending(x => x.RotatedAtUtc)
            .Skip((page - 1) * pageSize)
            .Limit(pageSize)
            .ToListAsync(cancellationToken);

        return new KeyRotationPage(items, totalCount);
    }

    private async Task EnsureIndexesAsync(CancellationToken cancellationToken)
    {
        if (_indexesInitialized)
        {
            return;
        }

        await _initSemaphore.WaitAsync(cancellationToken);
        try
        {
            if (_indexesInitialized)
            {
                return;
            }

            // At most one Active record at any time (guards racing activations across instances).
            var uniqueActive = new CreateIndexModel<KeyRotationRecord>(
                Builders<KeyRotationRecord>.IndexKeys.Ascending(x => x.Status),
                new CreateIndexOptions<KeyRotationRecord>
                {
                    Unique = true,
                    Name = "ux_status_active",
                    PartialFilterExpression = Builders<KeyRotationRecord>.Filter.Eq(x => x.Status, KeyRotationStatus.Active)
                });

            var byRotatedAt = new CreateIndexModel<KeyRotationRecord>(
                Builders<KeyRotationRecord>.IndexKeys.Descending(x => x.RotatedAtUtc),
                new CreateIndexOptions { Name = "ix_rotated_at_desc" });

            await _collection.Indexes.CreateManyAsync([uniqueActive, byRotatedAt], cancellationToken);
            _indexesInitialized = true;
        }
        finally
        {
            _initSemaphore.Release();
        }
    }

    public void Dispose() => _initSemaphore.Dispose();
}
