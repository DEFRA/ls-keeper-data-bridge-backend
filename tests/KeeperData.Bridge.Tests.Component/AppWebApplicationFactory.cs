using Amazon.S3;
using Amazon.S3.Model;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;
using System.Net;

namespace KeeperData.Bridge.Tests.Component;

public class AppWebApplicationFactory : WebApplicationFactory<Program>
{
    public Mock<IAmazonS3>? AmazonS3Mock;
    public Mock<IS3ClientFactory>? S3ClientFactoryMock;

    private const string ExternalStorageBucket = "test-external-bucket";

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        SetTestEnvironmentVariables();

        builder.ConfigureTestServices(services =>
        {
            RemoveService<IHealthCheckPublisher>(services);

            ConfigureS3ClientFactory(services);
        });
    }

    private static void SetTestEnvironmentVariables()
    {
        Environment.SetEnvironmentVariable("LocalStack_ServiceURL", "http://localhost:4566");
        Environment.SetEnvironmentVariable("IMB_S3_ACCESS_KEY", "test");
        Environment.SetEnvironmentVariable("IMB_S3_ACCESS_SECRET", "test");
        Environment.SetEnvironmentVariable("StorageConfiguration__ExternalStorage__BucketName", ExternalStorageBucket);
    }

    private void ConfigureS3ClientFactory(IServiceCollection services)
    {
        AmazonS3Mock = new Mock<IAmazonS3>();
        AmazonS3Mock
            .Setup(x => x.GetBucketAclAsync(It.IsAny<GetBucketAclRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetBucketAclResponse { HttpStatusCode = HttpStatusCode.OK });

        var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IS3ClientFactory>();

        if (factory is S3ClientFactory concreteFactory)
        {
            concreteFactory.RegisterMockClient<ExternalStorageClient>(ExternalStorageBucket, AmazonS3Mock.Object);
        }
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