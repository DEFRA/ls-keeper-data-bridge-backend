using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Database.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddDatabaseDependencies(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<MongoConfig>(configuration.GetSection("Mongo"));
    }
}