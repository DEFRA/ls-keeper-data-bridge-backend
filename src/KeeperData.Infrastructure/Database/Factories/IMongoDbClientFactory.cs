using MongoDB.Driver;

namespace KeeperData.Infrastructure.Database.Factories;

public interface IMongoDbClientFactory
{
    IMongoClient CreateClient();
}