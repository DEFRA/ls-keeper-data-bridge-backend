using KeeperData.SamAPI.Security;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KeeperData.SamAPI.Tests.Integration
{
    public class SamApiFixture
    {
        public ISamApi SamApi { get; }

        public SamApiFixture()
        {
            var configuration = new ConfigurationBuilder()
                .AddUserSecrets<SamApiFixture>()
                .AddEnvironmentVariables()
                .Build();

            var services = new ServiceCollection();

            services.AddSingleton<IConfiguration>(configuration);

            services.Configure<SamApiOptions>(
                configuration.GetSection("SamApi"));

            services.AddHttpClient<ITokenClient, CognitoTokenClient>(client =>
            {
                client.BaseAddress = new Uri(configuration["SamApi:TokenUrl"]!);
            });

            services.AddHttpClient<ISamApi, SamApi>(client =>
            {
                client.BaseAddress = new Uri(configuration["SamApi:BaseUrl"]!);
            });

            var provider = services.BuildServiceProvider();

            SamApi = provider.GetRequiredService<ISamApi>();
        }
    }
}