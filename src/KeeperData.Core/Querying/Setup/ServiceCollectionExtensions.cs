using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Impl;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Querying.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the query services for dynamic collection querying
    /// </summary>
    public static IServiceCollection AddMongoQueryService(this IServiceCollection services)
    {
        services.AddScoped<IQueryService, QueryService>();
        services.AddScoped<IODataQueryService, ODataQueryService>();

        return services;
    }
}