using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace KeeperData.Bridge.Tests.Integration;

public class AppWebApplicationFactory : WebApplicationFactory<Program>
{
    private IHost? _host;

    protected override IHost CreateHost(IHostBuilder builder)
    {
        Environment.SetEnvironmentVariable("LocalStack_ServiceURL", "http://localhost:4566");
        Environment.SetEnvironmentVariable("IMB_S3_ACCESS_KEY", "test");
        Environment.SetEnvironmentVariable("IMB_S3_ACCESS_SECRET", "test");
        Environment.SetEnvironmentVariable("StorageConfiguration__ExternalStorage__BucketName", "test-external-bucket");

        builder.ConfigureServices(services =>
        {
            RemoveService<IHealthCheckPublisher>(services);
        });

        _host = base.CreateHost(builder);

        return _host;
    }

    private static void RemoveService<T>(IServiceCollection services)
    {
        var service = services.FirstOrDefault(x => x.ServiceType == typeof(T));
        if (service != null)
        {
            services.Remove(service);
        }
    }
}