using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Impl;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Querying.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the MongoDB query service for dynamic collection querying
    /// </summary>
    public static IServiceCollection AddMongoQueryService(this IServiceCollection services)
    {
        services.AddScoped<IMongoQueryService, MongoQueryService>();

        return services;
    }
}