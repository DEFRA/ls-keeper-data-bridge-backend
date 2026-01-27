using System.Security.Cryptography;
using System.Text;
using KeeperData.Infrastructure.Notifications.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Notify.Exceptions;

namespace KeeperData.Infrastructure.Notifications;

/// <summary>
/// Health check for GOV.UK Notify service connectivity.
/// </summary>
public class GovukNotifyHealthCheck : IHealthCheck
{
    private readonly CleanseReportNotificationConfig _config;
    private readonly INotificationClientFactory _clientFactory;
    private readonly ILogger<GovukNotifyHealthCheck> _logger;

    public GovukNotifyHealthCheck(
        IOptions<CleanseReportNotificationConfig> config,
        INotificationClientFactory clientFactory,
        ILogger<GovukNotifyHealthCheck> logger)
    {
        _config = config.Value;
        _clientFactory = clientFactory;
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

        if (!_config.HasApiKey)
        {
            _logger.LogWarning("GOV.UK Notify health check skipped - API key not configured");
            return HealthCheckResult.Healthy("API key not configured", data: data);
        }

        try
        {
            _logger.LogDebug("Checking GOV.UK Notify connectivity by fetching templates");

            var client = _clientFactory.Create(_config.ApiKey);
            var templates = await client.GetAllTemplatesAsync()
                .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);

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
            var fullMessage = GetFullExceptionMessage(ex);
            _logger.LogError(ex, "GOV.UK Notify client error during health check: {ErrorMessage}", fullMessage);
            data["Error"] = fullMessage;
            return HealthCheckResult.Unhealthy($"Client error: {fullMessage}", ex, data);
        }
        catch (TimeoutException ex)
        {
            _logger.LogWarning(ex, "GOV.UK Notify health check timed out");
            data["Error"] = "Request timed out";
            return HealthCheckResult.Degraded("Request timed out - service may be slow or unreachable", data: data);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation("GOV.UK Notify health check was cancelled");
            throw; // Let the health check system handle cancellation
        }
        catch (Exception ex)
        {
            var fullMessage = GetFullExceptionMessage(ex);
            _logger.LogError(ex, "Unexpected error during GOV.UK Notify health check: {ErrorMessage}", fullMessage);
            data["Error"] = fullMessage;
            return HealthCheckResult.Unhealthy($"Unexpected error: {fullMessage}", ex, data);
        }
    }

    private static string HashApiKey(string apiKey)
    {
        if (string.IsNullOrEmpty(apiKey))
            return "(not configured)";

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(apiKey));
        return Convert.ToHexString(bytes)[..16].ToLowerInvariant();
    }

    private static string GetFullExceptionMessage(Exception ex)
    {
        var messages = new List<string>();
        var current = ex;
        
        while (current != null)
        {
            messages.Add(current.Message);
            current = current.InnerException;
        }
        
        return string.Join(" --> ", messages);
    }
}
