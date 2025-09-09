using Amazon.Extensions.NETCore.Setup;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;
using System.Net;

namespace KeeperData.Bridge.Tests.Component;

public class AppWebApplicationFactory : WebApplicationFactory<Program>
{
    public Mock<IAmazonS3>? AmazonS3Mock;

    public Mock<IAmazonSimpleNotificationService>? AmazonSimpleNotificationServiceMock;

    private const string ExternalStorageBucket = "test-external-bucket";
    private const string DataBridgeEventsTopicName = "ls-keeper-data-bridge-events";
    private const string DataBridgeEventsTopicArn = $"arn:aws:sns:eu-west-2:000000000000:{DataBridgeEventsTopicName}";

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        SetTestEnvironmentVariables();

        builder.ConfigureTestServices(services =>
        {
            RemoveService<IHealthCheckPublisher>(services);

            ConfigureAwsOptions(services);

            ConfigureS3ClientFactory(services);

            ConfigureSimpleNotificationService(services);
        });
    }

    private static void SetTestEnvironmentVariables()
    {
        Environment.SetEnvironmentVariable("AWS__ServiceURL", "http://localhost:4566");
        Environment.SetEnvironmentVariable("IMB_S3_ACCESS_KEY", "test");
        Environment.SetEnvironmentVariable("IMB_S3_ACCESS_SECRET", "test");
        Environment.SetEnvironmentVariable("StorageConfiguration__ExternalStorage__BucketName", ExternalStorageBucket);
        Environment.SetEnvironmentVariable("ServiceBusSenderConfiguration__DataBridgeEventsTopic__TopicName", DataBridgeEventsTopicName);
        Environment.SetEnvironmentVariable("ServiceBusSenderConfiguration__DataBridgeEventsTopic__TopicArn", string.Empty);
    }

    private static void ConfigureAwsOptions(IServiceCollection services)
    {
        var provider = services.BuildServiceProvider();
        var awsOptions = provider.GetRequiredService<AWSOptions>();
        awsOptions.Credentials = new BasicAWSCredentials("test", "test");
        services.Replace(new ServiceDescriptor(typeof(AWSOptions), awsOptions));
    }

    private void ConfigureS3ClientFactory(IServiceCollection services)
    {
        AmazonS3Mock = new Mock<IAmazonS3>();

        AmazonS3Mock
            .Setup(x => x.GetBucketAclAsync(It.IsAny<GetBucketAclRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetBucketAclResponse { HttpStatusCode = HttpStatusCode.OK });

        AmazonS3Mock
            .Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.OK });

        var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IS3ClientFactory>();

        if (factory is S3ClientFactory concreteFactory)
        {
            concreteFactory.RegisterMockClient<ExternalStorageClient>(ExternalStorageBucket, AmazonS3Mock.Object);
        }
    }

    private void ConfigureSimpleNotificationService(IServiceCollection services)
    {
        RemoveService<IAmazonSimpleNotificationService>(services);

        AmazonSimpleNotificationServiceMock = new Mock<IAmazonSimpleNotificationService>();

        AmazonSimpleNotificationServiceMock
            .Setup(x => x.ListTopicsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListTopicsResponse { HttpStatusCode = HttpStatusCode.OK, Topics = [new Topic { TopicArn = DataBridgeEventsTopicArn }] });

        AmazonSimpleNotificationServiceMock
            .Setup(x => x.GetTopicAttributesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetTopicAttributesResponse { HttpStatusCode = HttpStatusCode.OK });

        services.Replace(new ServiceDescriptor(typeof(IAmazonSimpleNotificationService), AmazonSimpleNotificationServiceMock.Object));
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