using MongoDB.Driver;

namespace KeeperData.Infrastructure.Database.Factories;

public interface IMongoSessionFactory
{
    IClientSessionHandle GetSession();
}