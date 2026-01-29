using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Setup;
using KeeperData.Infrastructure.Notifications;
using KeeperData.Infrastructure.Notifications.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Setup;

[ExcludeFromCodeCoverage]
public static class CleanseReportServiceCollectionExtensions
{
    /// <summary>
    /// Adds all cleanse report dependencies including core and infrastructure components.
    /// </summary>
    public static void AddCleanseReportServices(this IServiceCollection services, IConfiguration configuration)
    {
        // Register concrete DataSetDefinitions (resolved from IDataSetDefinitions which is registered by AddEtlDependencies)
        services.AddSingleton<DataSetDefinitions>(sp =>
            (DataSetDefinitions)sp.GetRequiredService<IDataSetDefinitions>());

        // Add core dependencies
        services.AddCleanseReportDependencies();

        // Add notification service and configuration
        services.Configure<CleanseReportNotificationConfig>(
            configuration.GetSection(CleanseReportNotificationConfig.SectionName));
        services.AddSingleton<INotificationClientFactory, NotificationClientFactory>();
        services.AddScoped<ICleanseReportNotificationService, GovukNotifyCleanseReportNotificationService>();

        // Add health check for GOV.UK Notify
        services.AddHealthChecks()
            .AddCheck<GovukNotifyHealthCheck>("govuk-notify", tags: ["notify", "external"]);
    }
}
