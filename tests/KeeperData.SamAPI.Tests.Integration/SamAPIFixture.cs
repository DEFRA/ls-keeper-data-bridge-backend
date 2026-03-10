using KeeperData.SamAPI.Security;
using Microsoft.Extensions.DependencyInjection;

namespace KeeperData.SamAPI.Tests.Integration
{
    public class SamApiFixture
    {
        public ISamApi SamApi { get; }

        public SamApiFixture()
        {
            var services = new ServiceCollection();

            services.AddHttpClient<ISamApi, SamApi>();
            services.AddSingleton<ITokenClient, CognitoTokenClient>();

            SamApi = services.BuildServiceProvider().GetRequiredService<ISamApi>();
        }
    }
}