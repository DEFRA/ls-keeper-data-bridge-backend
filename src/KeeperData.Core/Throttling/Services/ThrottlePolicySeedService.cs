using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Throttling.Abstract;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Throttling.Services;

[ExcludeFromCodeCoverage(Justification = "One-time seed service - covered by integration tests.")]
public sealed class ThrottlePolicySeedService(
    IServiceScopeFactory scopeFactory,
    ILogger<ThrottlePolicySeedService> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var scope = scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IThrottlePolicyRepository>();

            var count = await repository.CountAsync(cancellationToken);
            if (count > 0)
            {
                logger.LogInformation("Throttle policies already exist ({Count}); skipping seed", count);
                return;
            }

            logger.LogInformation("Seeding {Count} default throttle policies", ThrottlePolicyDefaults.SeedPolicies.Count);

            foreach (var policy in ThrottlePolicyDefaults.SeedPolicies)
            {
                await repository.UpsertAsync(policy, cancellationToken);
                logger.LogInformation("Seeded throttle policy: '{Name}' (slug: {Slug})", policy.Name, policy.Slug);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to seed throttle policies; will use Normal fallback");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
