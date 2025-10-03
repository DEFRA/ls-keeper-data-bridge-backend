using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.ETL.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddEtlDependencies(this IServiceCollection services)
    {
        services.AddSingleton<IDataSetDefinitions>(_ => StandardDataSetDefinitionsBuilder.Build());
        services.AddTransient<ISourceDataServiceFactory, SourceDataServiceFactory>();
    }
}