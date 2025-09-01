using KeeperData.Infrastructure.Database.Setup;
using KeeperData.Infrastructure.Messaging.Setup;
using KeeperData.Infrastructure.Storage.Setup;

namespace KeeperData.Bridge.Setup
{
    public static class ServiceCollectionExtensions
    {
        public static void ConfigureApi(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddDatabaseDependencies(configuration);

            services.AddMessagingDependencies(configuration);

            services.AddStorageDependencies(configuration);

            services.ConfigureHealthChecks();
        }

        private static void ConfigureHealthChecks(this IServiceCollection services)
        {
            services.AddHealthChecks();
        }
    }
}
