using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Throttling.Abstract;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Throttling.Services;

[ExcludeFromCodeCoverage(Justification = "Background polling service - covered by integration tests.")]
public sealed class ThrottlePolicyPollingService(
    IServiceScopeFactory scopeFactory,
    IThrottlePolicyProvider provider,
    ILogger<ThrottlePolicyPollingService> logger) : BackgroundService
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(20);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Throttle policy polling service started (interval: {Interval}s)", PollInterval.TotalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PollAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to poll throttle policy; retaining current settings");
            }

            await Task.Delay(PollInterval, stoppingToken);
        }
    }

    private async Task PollAsync(CancellationToken ct)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IThrottlePolicyRepository>();

        var active = await repository.GetActiveAsync(ct);
        var previousName = provider.ActivePolicyName;

        provider.Refresh(active);

        if (provider.ActivePolicyName != previousName)
        {
            logger.LogInformation("Throttle policy changed: '{Previous}' → '{Current}'", previousName, provider.ActivePolicyName);
        }
    }
}
