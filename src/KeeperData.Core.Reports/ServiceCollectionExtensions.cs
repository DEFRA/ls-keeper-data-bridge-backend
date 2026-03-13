using KeeperData.Core.Reports.Cleanse.Analysis.Command;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Impl;
using KeeperData.Core.Reports.Cleanse.Export.Command;
using KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Export.Index;
using KeeperData.Core.Reports.Cleanse.Export.Metadata;
using KeeperData.Core.Reports.Cleanse.Export.Metadata.Abstract;
using KeeperData.Core.Reports.Cleanse.Export.Operations;
using KeeperData.Core.Reports.Cleanse.Export.Operations.Abstract;
using KeeperData.Core.Reports.Cleanse.Export.Operations.Repositories;
using KeeperData.Core.Reports.Cleanse.Operations.Command;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Repositories;
using KeeperData.Core.Reports.Cleanse.Operations.Index;
using KeeperData.Core.Reports.Cleanse.Operations.Queries;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Abstract;
using KeeperData.Core.Reports.Initialisation;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Issues.Command;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Issues.Command.Repositories;
using KeeperData.Core.Reports.Issues.Index;
using KeeperData.Core.Reports.Issues.Query;
using KeeperData.Core.Reports.Issues.Query.Abstract;
using KeeperData.Core.Reports.SamCtsHoldings.Query;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Abstract;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports;

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
        // Register shared collection accessors
        services.AddSingleton<IssueCollection>();
        services.AddSingleton<IssueHistoryCollection>();
        services.AddSingleton<CleanseOperationsCollection>();
        services.AddSingleton<ExportMetadataCollection>();
        services.AddSingleton<CleanseExportOperationsCollection>();

        // Register repositories
        services.AddScoped<IIssueAggRootRepository, IssueAggRootRepository>();
        services.AddScoped<IIssueHistoryAggRootRepository, IssueHistoryAggRootRepository>();
        services.AddScoped<ICleanseAnalysisOperationAggRootRepository, CleanseAnalysisOperationAggRootRepository>();
        services.AddScoped<ICleanseAnalysisOperationsQueries, CleanseAnalysisOperationsQueries>();
        services.AddScoped<IExportMetadataRepository, ExportMetadataRepository>();
        services.AddScoped<ICleanseExportOperationRepository, CleanseExportOperationRepository>();

        // Register command services
        services.AddScoped<IIssueCommandService, IssueCommandService>();
        services.AddScoped<ICleanseOperationCommandService, CleanseOperationCommandService>();

        // Register query services
        services.AddScoped<IIssueQueries, IssueQueries>();
        services.AddScoped<ICleanseExportOperationQueries, CleanseExportOperationQueries>();

        // Register run stats service (singleton - holds in-memory sliding window state)
        services.AddSingleton<ICleanseRunStatsService, CleanseRunStatsService>();

        // Register index managers and initialisation
        services.AddSingleton<IIssueIndexManager, IssueIndexManager>();
        services.AddSingleton<IIssueHistoryIndexManager, IssueHistoryIndexManager>();
        services.AddSingleton<ICleanseOperationsIndexManager, CleanseOperationsIndexManager>();
        services.AddSingleton<ICleanseExportOperationsIndexManager, CleanseExportOperationsIndexManager>();
        services.AddSingleton<ICleanseReportInitialisation, CleanseReportInitialisation>();

        // Register query services for CTS/SAM data
        services.AddScoped<ICtsSamQueryService, CtsSamQueryService>();

        // Register engine
        services.AddScoped<ICleanseAnalysisEngine, CleanseAnalysisEngine>();

        // Register core services
        services.AddScoped<ICleanseAnalysisCommandService, CleanseAnalysisCommandService>();

        // Register export services (requires ICleanseReportPresignedUrlGenerator to be registered by infrastructure)
        services.AddScoped<ICleanseReportExportCommandService, CleanseReportExportCommandService>();
        services.AddScoped<ICleanseExportCommandService, CleanseExportCommandService>();

        // Register facade
        services.AddScoped<ICleanseFacadeCommands, CleanseFacadeCommands>();
        services.AddScoped<ICleanseFacadeQueries, CleanseFacadeQueries>();
        services.AddScoped<ICleanseFacade, CleanseFacade>();
    }
}
