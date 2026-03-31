using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Impl;
using KeeperData.Core.Throttling.Persistence;
using KeeperData.Core.Throttling.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KeeperData.Core.Throttling.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddThrottlePolicies(this IServiceCollection services, IConfiguration configuration)
    {
        var useZeroThrottle = configuration.GetValue<bool>("FeatureFlags:UseZeroThrottle");

        services.AddSingleton<ThrottlePolicyCollection>();
        services.AddScoped<IThrottlePolicyRepository, ThrottlePolicyRepository>();

        var provider = new ThrottlePolicyProvider();
        services.AddSingleton<IThrottlePolicyProvider>(provider);

        services.AddScoped<IThrottlePolicyQueryService, ThrottlePolicyQueryService>();
        services.AddScoped<IThrottlePolicyCommandService, ThrottlePolicyCommandService>();

        if (useZeroThrottle)
        {
            services.AddSingleton<IThrottler, ZeroThrottler>();
            return;
        }

        services.AddSingleton<IThrottleDelay, ThrottleDelay>();
        services.AddSingleton<IThrottler, Throttler>();

        services.AddHostedService<ThrottlePolicySeedService>();
        services.AddHostedService<ThrottlePolicyPollingService>();
    }
}
