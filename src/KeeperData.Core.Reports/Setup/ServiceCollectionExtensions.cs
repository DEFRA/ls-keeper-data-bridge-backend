using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Impl;
using KeeperData.Core.Reports.Querying;
using KeeperData.Core.Reports.Strategies;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds core cleanse report dependencies. 
    /// Note: ICleanseReportExportService and ICleanseReportPresignedUrlGenerator must be registered separately
    /// as they depend on infrastructure (S3) components.
    /// </summary>
    public static void AddCleanseReportDependencies(this IServiceCollection services)
    {
        // Register repositories
        services.AddScoped<ICleanseReportRepository, CleanseReportRepository>();
        services.AddScoped<ICleanseAnalysisRepository, CleanseAnalysisRepository>();

        // Register analysis strategies
        services.AddScoped<ICleanseAnalysisStrategy, CtsSamAnalysisStrategy>();

        // Register core services
        services.AddScoped<ICleanseReportService, CleanseReportService>();
        services.AddScoped<ICleanseIssueQueryService, CleanseIssueQueryService>();

        // Register export service (requires ICleanseReportPresignedUrlGenerator to be registered by infrastructure)
        services.AddScoped<ICleanseReportExportService, CleanseReportExportService>();
    }
}
