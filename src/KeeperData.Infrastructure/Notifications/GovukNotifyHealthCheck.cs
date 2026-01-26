using System.Security.Cryptography;
using System.Text;
using KeeperData.Infrastructure.Notifications.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Notify.Client;
using Notify.Exceptions;

namespace KeeperData.Infrastructure.Notifications;

/// <summary>
/// Health check for GOV.UK Notify service connectivity.
/// </summary>
public class GovukNotifyHealthCheck : IHealthCheck
{
    private readonly CleanseReportNotificationConfig _config;
    private readonly ILogger<GovukNotifyHealthCheck> _logger;

    public GovukNotifyHealthCheck(
        IOptions<CleanseReportNotificationConfig> config,
        ILogger<GovukNotifyHealthCheck> logger)
    {
        _config = config.Value;
        _logger = logger;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var data = new Dictionary<string, object>
        {
            ["TemplateId"] = _config.TemplateId,
            ["ApiKeyHash"] = HashApiKey(_config.ApiKey),
            ["RecipientEmails"] = _config.GetMaskedRecipientEmails(),
            ["Enabled"] = _config.Enabled
        };

        if (!_config.Enabled)
        {
            _logger.LogInformation("GOV.UK Notify health check skipped - notifications are disabled");
            return HealthCheckResult.Healthy("Notifications disabled", data);
        }

        if (string.IsNullOrEmpty(_config.ApiKey))
        {
            _logger.LogWarning("GOV.UK Notify health check failed - API key not configured");
            return HealthCheckResult.Unhealthy("API key not configured", data: data);
        }

        try
        {
            _logger.LogDebug("Checking GOV.UK Notify connectivity by fetching templates");

            var client = new NotificationClient(_config.ApiKey);
            var templates = await client.GetAllTemplatesAsync();

            var templateCount = templates.templates?.Count ?? 0;
            data["TemplateCount"] = templateCount;

            // Verify the configured template exists
            var configuredTemplateExists = templates.templates?
                .Any(t => t.id == _config.TemplateId) ?? false;
            data["ConfiguredTemplateExists"] = configuredTemplateExists;

            if (!configuredTemplateExists)
            {
                _logger.LogWarning(
                    "GOV.UK Notify is reachable but configured template {TemplateId} was not found among {TemplateCount} templates",
                    _config.TemplateId, templateCount);

                return HealthCheckResult.Degraded(
                    $"Connected but configured template not found (found {templateCount} templates)",
                    data: data);
            }

            _logger.LogDebug(
                "GOV.UK Notify health check passed - found {TemplateCount} templates including configured template",
                templateCount);

            return HealthCheckResult.Healthy(
                $"Connected successfully ({templateCount} templates available)",
                data);
        }
        catch (NotifyClientException ex)
        {
            _logger.LogError(ex, "GOV.UK Notify client error during health check");
            data["Error"] = ex.Message;
            return HealthCheckResult.Unhealthy($"Client error: {ex.Message}", ex, data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error during GOV.UK Notify health check");
            data["Error"] = ex.Message;
            return HealthCheckResult.Unhealthy($"Unexpected error: {ex.Message}", ex, data);
        }
    }

    private static string HashApiKey(string apiKey)
    {
        if (string.IsNullOrEmpty(apiKey))
            return "(not configured)";

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(apiKey));
        return Convert.ToHexString(bytes)[..16].ToLowerInvariant();
    }
}
