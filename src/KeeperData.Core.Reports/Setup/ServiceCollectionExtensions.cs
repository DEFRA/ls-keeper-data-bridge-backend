using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Analysis;
using KeeperData.Core.Reports.Impl;
using KeeperData.Core.Reports.Querying;
using KeeperData.Core.Reports.Strategies;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddCleanseReportDependencies(this IServiceCollection services)
    {
        // Register repositories
        services.AddScoped<ICleanseReportRepository, CleanseReportRepository>();
        services.AddScoped<ICleanseAnalysisRepository, CleanseAnalysisRepository>();

        // Register analysis strategies
        services.AddScoped<ICleanseAnalysisStrategy, CtsSamAnalysisStrategy>();

        // Register services
        services.AddScoped<ICleanseReportService, CleanseReportService>();
        services.AddScoped<ICleanseIssueQueryService, CleanseIssueQueryService>();
    }
}
