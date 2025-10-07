using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Domain.Entities;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Locking;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Infrastructure.Locking;

/// <summary>
/// Integration tests for MongoDistributedLock using MongoDB testcontainer
/// </summary>
[Collection("MongoDB"), Trait("Dependence", "docker")]
public class MongoDistLockIntegrationTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly MongoDistributedLock _distributedLock;
    private readonly IMongoCollection<DistributedLock> _locksCollection;

    public MongoDistLockIntegrationTests(ITestOutputHelper testOutputHelper, MongoDbFixture mongoDbFixture)
    {
        _testOutputHelper = testOutputHelper;
        _mongoDbFixture = mongoDbFixture;

        var mongoConfig = Options.Create(new MongoConfig
        {
            DatabaseName = MongoDbFixture.TestDatabaseName,
            DatabaseUri = _mongoDbFixture.ConnectionString
        });

        _distributedLock = new MongoDistributedLock(mongoConfig, _mongoDbFixture.MongoClient);
        
        var database = _mongoDbFixture.MongoClient.GetDatabase(MongoDbFixture.TestDatabaseName);
        _locksCollection = database.GetCollection<DistributedLock>("distributed_locks");
    }

    public async Task InitializeAsync()
    {
        // Initialize the distributed lock to create TTL index
        await _distributedLock.InitializeAsync();
    }

    public async Task DisposeAsync()
    {
        // Clean up all locks from this test run
        await _locksCollection.DeleteManyAsync(Builders<DistributedLock>.Filter.Empty);
        _distributedLock?.Dispose();
    }

    #region TTL Index Tests

    [Fact]
    public async Task InitializeAsync_ShouldCreateTtlIndex()
    {
        // Arrange - Get indexes from collection
        var indexes = await _locksCollection.Indexes.ListAsync();
        var indexList = await indexes.ToListAsync();

        // Assert - Should have at least one index (TTL + default _id)
        indexList.Should().HaveCountGreaterThan(1);

        // Find the TTL index
        var ttlIndex = indexList.FirstOrDefault(idx =>
        {
            if (idx.TryGetValue("key", out var keyValue))
            {
                var keyDoc = keyValue.AsBsonDocument;
                return keyDoc.Contains("ExpiresAtUtc");
            }
            return false;
        });

        ttlIndex.Should().NotBeNull("TTL index on ExpiresAtUtc should exist");
        
        // Verify expireAfterSeconds is 0 (expires at the specified time)
        if (ttlIndex!.TryGetValue("expireAfterSeconds", out var expireValue))
        {
            expireValue.ToInt32().Should().Be(0);
        }

        _testOutputHelper.WriteLine($"Found {indexList.Count} indexes on distributed_locks collection");
    }

    [Fact]
    public async Task InitializeAsync_CalledMultipleTimes_ShouldBeIdempotent()
    {
        // Act - Call initialize multiple times
        await _distributedLock.InitializeAsync();
        await _distributedLock.InitializeAsync();
        await _distributedLock.InitializeAsync();

        // Assert - Should not throw and indexes should still be valid
        var indexes = await _locksCollection.Indexes.ListAsync();
        var indexList = await indexes.ToListAsync();
        indexList.Should().HaveCountGreaterThan(1);
    }

    #endregion

    #region Basic Lock Acquisition Tests

    [Fact]
    public async Task TryAcquireAsync_WhenLockDoesNotExist_ShouldAcquireLock()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        var duration = TimeSpan.FromMinutes(1);

        // Act
        await using var lockHandle = await _distributedLock.TryAcquireAsync(lockName, duration);

        // Assert
        lockHandle.Should().NotBeNull();

        // Verify lock exists in database
        var lockDoc = await _locksCollection.Find(l => l.Id == lockName).FirstOrDefaultAsync();
        lockDoc.Should().NotBeNull();
        lockDoc!.Owner.Should().NotBeNullOrEmpty();
        lockDoc.ExpiresAtUtc.Should().BeCloseTo(DateTimeOffset.UtcNow.Add(duration), TimeSpan.FromSeconds(5));

        _testOutputHelper.WriteLine($"Lock acquired: {lockName}, Owner: {lockDoc.Owner}, Expires: {lockDoc.ExpiresAtUtc}");
    }

    [Fact]
    public async Task TryAcquireAsync_WhenLockExists_ShouldReturnNull()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        var duration = TimeSpan.FromMinutes(1);

        // First acquisition
        await using var firstLock = await _distributedLock.TryAcquireAsync(lockName, duration);
        firstLock.Should().NotBeNull();

        // Act - Try to acquire the same lock
        await using var secondLock = await _distributedLock.TryAcquireAsync(lockName, duration);

        // Assert
        secondLock.Should().BeNull("lock is already held by first acquisition");

        _testOutputHelper.WriteLine($"Second acquisition correctly blocked for lock: {lockName}");
    }

    [Fact]
    public async Task TryAcquireAsync_WhenLockExpired_ShouldReplaceAndAcquire()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        
        // Create an expired lock directly in database
        var expiredLock = new DistributedLock
        {
            Id = lockName,
            Owner = "expired-owner",
            ExpiresAtUtc = DateTimeOffset.UtcNow.AddMinutes(-5)
        };
        await _locksCollection.InsertOneAsync(expiredLock);

        // Act - Try to acquire the expired lock
        await using var lockHandle = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));

        // Assert
        lockHandle.Should().NotBeNull("expired lock should be replaceable");

        // Verify the lock was replaced
        var currentLock = await _locksCollection.Find(l => l.Id == lockName).FirstOrDefaultAsync();
        currentLock.Should().NotBeNull();
        currentLock!.Owner.Should().NotBe("expired-owner");
        currentLock.ExpiresAtUtc.Should().BeAfter(DateTimeOffset.UtcNow);

        _testOutputHelper.WriteLine($"Expired lock replaced. Old owner: expired-owner, New owner: {currentLock.Owner}");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public async Task TryAcquireAsync_WithInvalidLockName_ShouldThrow(string? lockName)
    {
        // Act & Assert
        await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
            await _distributedLock.TryAcquireAsync(lockName!, TimeSpan.FromMinutes(1)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public async Task TryAcquireAsync_WithInvalidDuration_ShouldThrow(int seconds)
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await _distributedLock.TryAcquireAsync("test-lock", TimeSpan.FromSeconds(seconds)));
    }

    #endregion

    #region Lock Release Tests

    [Fact]
    public async Task DisposeAsync_WhenCalled_ShouldReleaseLock()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        var lockHandle = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));
        lockHandle.Should().NotBeNull();

        // Verify lock exists
        var lockBeforeDispose = await _locksCollection.Find(l => l.Id == lockName).FirstOrDefaultAsync();
        lockBeforeDispose.Should().NotBeNull();

        // Act
        await lockHandle!.DisposeAsync();

        // Assert - Lock should be removed from database
        var lockAfterDispose = await _locksCollection.Find(l => l.Id == lockName).FirstOrDefaultAsync();
        lockAfterDispose.Should().BeNull("lock should be deleted after disposal");

        _testOutputHelper.WriteLine($"Lock successfully released: {lockName}");
    }

    [Fact]
    public async Task DisposeAsync_CalledMultipleTimes_ShouldBeIdempotent()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        var lockHandle = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));
        lockHandle.Should().NotBeNull();

        // Act - Dispose multiple times
        await lockHandle!.DisposeAsync();
        await lockHandle.DisposeAsync();
        await lockHandle.DisposeAsync();

        // Assert - Should not throw
        var lockAfterDispose = await _locksCollection.Find(l => l.Id == lockName).FirstOrDefaultAsync();
        lockAfterDispose.Should().BeNull();
    }

    [Fact]
    public async Task LockRelease_WhenAnotherProcessAcquired_ShouldNotDeleteTheirLock()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        
        // First process acquires lock
        var firstLock = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromSeconds(1));
        firstLock.Should().NotBeNull();

        // Wait for first lock to expire
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Second process acquires the expired lock
        var secondLock = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));
        secondLock.Should().NotBeNull();

        // Get the owner of second lock
        var secondLockDoc = await _locksCollection.Find(l => l.Id == lockName).FirstOrDefaultAsync();
        var secondOwner = secondLockDoc!.Owner;

        // Act - First process tries to release (should be no-op)
        await firstLock!.DisposeAsync();

        // Assert - Second lock should still exist
        var lockAfterFirstDispose = await _locksCollection.Find(l => l.Id == lockName).FirstOrDefaultAsync();
        lockAfterFirstDispose.Should().NotBeNull("second process still owns the lock");
        lockAfterFirstDispose!.Owner.Should().Be(secondOwner);

        // Cleanup
        await secondLock!.DisposeAsync();

        _testOutputHelper.WriteLine($"First process correctly did not delete second process's lock");
    }

    #endregion

    #region Lock Renewal Tests

    [Fact]
    public async Task TryRenewAsync_WhenLockIsOwned_ShouldExtendExpiration()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        await using var lockHandle = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));
        lockHandle.Should().NotBeNull();

        var originalLock = await _locksCollection.Find(l => l.Id == lockName).FirstOrDefaultAsync();
        var originalExpiry = originalLock!.ExpiresAtUtc;

        // Wait a bit
        await Task.Delay(TimeSpan.FromSeconds(1));

        // Act - Renew the lock
        var renewed = await lockHandle!.TryRenewAsync(TimeSpan.FromMinutes(5));

        // Assert
        renewed.Should().BeTrue();

        var renewedLock = await _locksCollection.Find(l => l.Id == lockName).FirstOrDefaultAsync();
        renewedLock.Should().NotBeNull();
        renewedLock!.ExpiresAtUtc.Should().BeAfter(originalExpiry);
        renewedLock.ExpiresAtUtc.Should().BeCloseTo(DateTimeOffset.UtcNow.AddMinutes(5), TimeSpan.FromSeconds(5));

        _testOutputHelper.WriteLine($"Lock renewed. Original expiry: {originalExpiry}, New expiry: {renewedLock.ExpiresAtUtc}");
    }

    [Fact]
    public async Task TryRenewAsync_WhenLockNotOwned_ShouldReturnFalse()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        var firstLock = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromSeconds(1));
        firstLock.Should().NotBeNull();

        // Wait for lock to expire
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Another process acquires the lock
        await using var secondLock = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));
        secondLock.Should().NotBeNull();

        // Act - First process tries to renew (doesn't own it anymore)
        var renewed = await firstLock!.TryRenewAsync(TimeSpan.FromMinutes(5));

        // Assert
        renewed.Should().BeFalse("lock is now owned by another process");

        // Cleanup
        await firstLock.DisposeAsync();
    }

    [Fact]
    public async Task TryRenewAsync_AfterDispose_ShouldReturnFalse()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        var lockHandle = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));
        lockHandle.Should().NotBeNull();

        // Dispose the lock
        await lockHandle!.DisposeAsync();

        // Act - Try to renew after disposal
        var renewed = await lockHandle.TryRenewAsync(TimeSpan.FromMinutes(5));

        // Assert
        renewed.Should().BeFalse("lock has been disposed");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public async Task TryRenewAsync_WithInvalidExtension_ShouldThrow(int seconds)
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        await using var lockHandle = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));
        lockHandle.Should().NotBeNull();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await lockHandle!.TryRenewAsync(TimeSpan.FromSeconds(seconds)));
    }

    #endregion

    #region Concurrency Tests

    [Fact]
    public async Task ConcurrentAcquisition_OnlyOneProcessShouldAcquire()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        var concurrentAttempts = 10;

        // Act - Try to acquire the same lock concurrently
        var tasks = Enumerable.Range(0, concurrentAttempts)
            .Select(_ => _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1)))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - Only one should succeed
        var successfulAcquisitions = results.Count(r => r != null);
        successfulAcquisitions.Should().Be(1, "only one process should acquire the lock");

        // Cleanup
        var successfulLock = results.FirstOrDefault(r => r != null);
        if (successfulLock != null)
        {
            await successfulLock.DisposeAsync();
        }

        _testOutputHelper.WriteLine($"Out of {concurrentAttempts} concurrent attempts, {successfulAcquisitions} succeeded (expected: 1)");
    }

    [Fact]
    public async Task SequentialAcquisition_AfterRelease_ShouldSucceed()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        var iterations = 5;

        for (var i = 0; i < iterations; i++)
        {
            // Act - Acquire lock
            await using var lockHandle = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));

            // Assert
            lockHandle.Should().NotBeNull($"iteration {i} should acquire lock");

            _testOutputHelper.WriteLine($"Iteration {i}: Lock acquired and will be released");
            
            // Lock is released here via DisposeAsync
        }

        // Final verification - no lock should remain
        var finalLock = await _locksCollection.Find(l => l.Id == lockName).FirstOrDefaultAsync();
        finalLock.Should().BeNull("all locks should be cleaned up");
    }

    [Fact]
    public async Task MultipleDistinctLocks_ShouldNotInterfere()
    {
        // Arrange
        var lock1Name = $"test-lock-1-{Guid.NewGuid()}";
        var lock2Name = $"test-lock-2-{Guid.NewGuid()}";
        var lock3Name = $"test-lock-3-{Guid.NewGuid()}";

        // Act - Acquire multiple different locks
        await using var lock1 = await _distributedLock.TryAcquireAsync(lock1Name, TimeSpan.FromMinutes(1));
        await using var lock2 = await _distributedLock.TryAcquireAsync(lock2Name, TimeSpan.FromMinutes(1));
        await using var lock3 = await _distributedLock.TryAcquireAsync(lock3Name, TimeSpan.FromMinutes(1));

        // Assert - All should succeed
        lock1.Should().NotBeNull();
        lock2.Should().NotBeNull();
        lock3.Should().NotBeNull();

        // Verify all exist in database
        var allLocks = await _locksCollection.Find(Builders<DistributedLock>.Filter.Empty).ToListAsync();
        allLocks.Count(l => l.Id == lock1Name || l.Id == lock2Name || l.Id == lock3Name).Should().Be(3);

        _testOutputHelper.WriteLine($"Successfully acquired {3} distinct locks simultaneously");
    }

    #endregion

    #region Edge Case Tests

    [Fact]
    public async Task LockAcquisition_WithVeryShortDuration_ShouldWorkCorrectly()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        var duration = TimeSpan.FromMilliseconds(100);

        // Act
        await using var lockHandle = await _distributedLock.TryAcquireAsync(lockName, duration);

        // Assert
        lockHandle.Should().NotBeNull();

        // Wait for expiration
        await Task.Delay(TimeSpan.FromMilliseconds(200));

        // Verify we can acquire again after expiration
        await using var secondLock = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));
        secondLock.Should().NotBeNull("lock should be expired and replaceable");

        _testOutputHelper.WriteLine("Short-duration lock worked correctly");
    }

    [Fact]
    public async Task LockAcquisition_WithLongDuration_ShouldWorkCorrectly()
    {
        // Arrange
        var lockName = $"test-lock-{Guid.NewGuid()}";
        var duration = TimeSpan.FromHours(24);

        // Act
        await using var lockHandle = await _distributedLock.TryAcquireAsync(lockName, duration);

        // Assert
        lockHandle.Should().NotBeNull();

        var lockDoc = await _locksCollection.Find(l => l.Id == lockName).FirstOrDefaultAsync();
        lockDoc.Should().NotBeNull();
        lockDoc!.ExpiresAtUtc.Should().BeCloseTo(DateTimeOffset.UtcNow.Add(duration), TimeSpan.FromSeconds(10));

        _testOutputHelper.WriteLine($"Long-duration lock created with expiry: {lockDoc.ExpiresAtUtc}");
    }

    [Fact]
    public async Task MultipleDistributedLockInstances_ShouldShareSameLocks()
    {
        // Arrange
        var mongoConfig = Options.Create(new MongoConfig
        {
            DatabaseName = MongoDbFixture.TestDatabaseName,
            DatabaseUri = _mongoDbFixture.ConnectionString
        });

        using var secondLock = new MongoDistributedLock(mongoConfig, _mongoDbFixture.MongoClient);
        await secondLock.InitializeAsync();

        var lockName = $"test-lock-{Guid.NewGuid()}";

        // Act - First instance acquires lock
        await using var firstHandle = await _distributedLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));
        firstHandle.Should().NotBeNull();

        // Second instance tries to acquire same lock
        await using var secondHandle = await secondLock.TryAcquireAsync(lockName, TimeSpan.FromMinutes(1));

        // Assert
        secondHandle.Should().BeNull("lock is held by first instance");

        _testOutputHelper.WriteLine("Multiple instances correctly share lock state");
    }

    #endregion
}
