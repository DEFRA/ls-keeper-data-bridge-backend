using KeeperData.Infrastructure.Metrics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.HealthCheck;

[ExcludeFromCodeCoverage]
public class HealthCheckPublisherHostedService : BackgroundService
{
    private readonly HealthCheckService _healthCheckService;
    private readonly HealthCheckMetricsPublisher _publisher;
    private readonly ILogger<HealthCheckPublisherHostedService> _logger;
    private readonly HealthCheckPublisherOptions _options;

    public HealthCheckPublisherHostedService(
        HealthCheckService healthCheckService,
        HealthCheckMetricsPublisher publisher,
        ILogger<HealthCheckPublisherHostedService> logger,
        IOptions<HealthCheckPublisherOptions> options)
    {
        _healthCheckService = healthCheckService;
        _publisher = publisher;
        _logger = logger;
        _options = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Initial delay before starting health check publishing
        if (_options.Delay > TimeSpan.Zero)
        {
            _logger.LogInformation("Health check publisher starting in {Delay}ms", _options.Delay.TotalMilliseconds);
            await Task.Delay(_options.Delay, stoppingToken);
        }

        _logger.LogInformation("Health check publisher started with {Period}ms interval", _options.Period.TotalMilliseconds);

        using var timer = new PeriodicTimer(_options.Period);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    var healthReport = await _healthCheckService.CheckHealthAsync(stoppingToken);
                    await _publisher.PublishAsync(healthReport, stoppingToken);

                    _logger.LogDebug("Published health check report: Status={Status}, Duration={Duration}ms",
                        healthReport.Status, healthReport.TotalDuration.TotalMilliseconds);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to publish health check report");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Health check publisher stopping due to cancellation");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Health check publisher stopping");
        await base.StopAsync(cancellationToken);
    }
}