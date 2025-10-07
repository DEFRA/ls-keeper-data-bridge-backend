using MongoDB.Driver;
using Testcontainers.MongoDb;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

/// <summary>
/// Fixture for sharing MongoDB container across distributed lock tests
/// </summary>
public class MongoDbFixture : IAsyncLifetime
{
    public MongoDbContainer Container { get; private set; } = null!;
    public IMongoClient MongoClient { get; private set; } = null!;
    public string ConnectionString { get; private set; } = null!;

    public const string TestDatabaseName = "test-distributed-locks";

    public async Task InitializeAsync()
    {
        Container = new MongoDbBuilder()
            .WithImage("mongo:7.0")
            .WithPortBinding(27017, true)
            .Build();

        await Container.StartAsync();

        ConnectionString = Container.GetConnectionString();
        MongoClient = new MongoClient(ConnectionString);

        // Verify connection
        await VerifyConnectionAsync();
    }

    public async Task DisposeAsync()
    {
        try
        {
            // Clean up database before disposing
            await MongoClient.DropDatabaseAsync(TestDatabaseName);
        }
        catch
        {
            // Ignore cleanup errors
        }
        finally
        {
            await Container.DisposeAsync();
        }
    }

    private async Task VerifyConnectionAsync()
    {
        var maxRetries = 5;
        var retryDelay = 1000;

        for (var i = 0; i < maxRetries; i++)
        {
            try
            {
                await MongoClient.GetDatabase(TestDatabaseName)
                    .RunCommandAsync<MongoDB.Bson.BsonDocument>(
                        new MongoDB.Bson.BsonDocument("ping", 1));
                return;
            }
            catch when (i < maxRetries - 1)
            {
                await Task.Delay(retryDelay);
                retryDelay *= 2;
            }
        }

        throw new InvalidOperationException("Failed to connect to MongoDB container");
    }
}