using Amazon.SimpleNotificationService;
using KeeperData.Infrastructure.Messaging.Configuration;
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
        var serviceBusSenderConfiguration = configuration.GetSection(nameof(ServiceBusSenderConfiguration)).Get<ServiceBusSenderConfiguration>()!;
        services.AddSingleton<IServiceBusSenderConfiguration>(serviceBusSenderConfiguration);

        if (configuration["LOCALSTACK_ENDPOINT"] != null)
        {
            services.AddSingleton<IAmazonSimpleNotificationService>(sp =>
            {
                var config = new AmazonSimpleNotificationServiceConfig
                {
                    ServiceURL = configuration["AWS:ServiceURL"],
                    AuthenticationRegion = configuration["AWS:Region"],
                    UseHttp = true
                };
                return new AmazonSimpleNotificationServiceClient(config);
            });
        }
        else
        {
            services.AddAWSService<IAmazonSimpleNotificationService>();
        }

        if (serviceBusSenderConfiguration.DataBridgeEventsTopic.HealthcheckEnabled)
        {
            services.AddHealthChecks()
                .AddCheck<AwsSnsHealthCheck>("aws_sns", tags: ["aws", "sns"]);
        }
    }
}