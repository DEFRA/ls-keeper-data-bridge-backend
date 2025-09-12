using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KeeperData.Infrastructure.Database.Factories.Implementations;

public class MongoDbClientFactory : IMongoDbClientFactory
{
    private readonly IOptions<MongoConfig> _mongoConfig;
    private IMongoClient? _cachedClient;
    private readonly object _lock = new();

    public MongoDbClientFactory(IOptions<MongoConfig> mongoConfig)
    {
        _mongoConfig = mongoConfig;

        if (string.IsNullOrWhiteSpace(mongoConfig?.Value?.DatabaseUri))
            throw new ArgumentException("MongoDB uri string cannot be empty");

        if (string.IsNullOrWhiteSpace(mongoConfig?.Value?.DatabaseName))
            throw new ArgumentException("MongoDB database name cannot be empty");
    }

    public IMongoClient CreateClient()
    {
        if (_cachedClient != null) return _cachedClient;

        lock (_lock)
        {
            if (_cachedClient == null)
            {
                var settings = MongoClientSettings.FromConnectionString(_mongoConfig.Value.DatabaseUri);
                _cachedClient = new MongoClient(settings);
            }
        }

        return _cachedClient;
    }
}