using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Messaging.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddMessagingDependencies(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddServiceBusSenderDependencies(configuration);
    }

    private static void AddServiceBusSenderDependencies(this IServiceCollection services, IConfiguration configuration)
    {
    }
}
