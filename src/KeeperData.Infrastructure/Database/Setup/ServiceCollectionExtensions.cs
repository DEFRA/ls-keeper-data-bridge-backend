using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KeeperData.Infrastructure.Database.Setup;

public static class ServiceCollectionExtensions
{
    public static void AddDatabaseDependencies(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<MongoConfig>(configuration.GetSection("Mongo"));
    }
}
