using KeeperData.Core.Reports.Abstract;
using KeeperData.Infrastructure.Notifications.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Notify.Client;
using Notify.Exceptions;

namespace KeeperData.Infrastructure.Notifications;

/// <summary>
/// GOV.UK Notify implementation for sending cleanse report notification emails.
/// </summary>
public class GovukNotifyCleanseReportNotificationService : ICleanseReportNotificationService
{
    private readonly CleanseReportNotificationConfig _config;
    private readonly ILogger<GovukNotifyCleanseReportNotificationService> _logger;

    public GovukNotifyCleanseReportNotificationService(
        IOptions<CleanseReportNotificationConfig> config,
        ILogger<GovukNotifyCleanseReportNotificationService> logger)
    {
        _config = config.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<CleanseReportNotificationResult> SendReportNotificationAsync(string reportUrl, CancellationToken ct = default)
    {
        var recipients = _config.GetRecipientEmailList();

        if (!_config.Enabled)
        {
            _logger.LogInformation("Cleanse report notifications are disabled. Skipping email send.");
            return new CleanseReportNotificationResult
            {
                Success = true,
                Recipient = string.Join("; ", recipients),
                Error = "Notifications disabled"
            };
        }

        if (string.IsNullOrEmpty(_config.ApiKey))
        {
            _logger.LogWarning("GOV.UK Notify API key is not configured. Cannot send cleanse report notification.");
            return new CleanseReportNotificationResult
            {
                Success = false,
                Recipient = string.Join("; ", recipients),
                Error = "API key not configured"
            };
        }

        if (recipients.Count == 0)
        {
            _logger.LogWarning("No recipient email addresses configured. Cannot send cleanse report notification.");
            return new CleanseReportNotificationResult
            {
                Success = false,
                Recipient = string.Empty,
                Error = "No recipient email addresses configured"
            };
        }

        var successfulRecipients = new List<string>();
        var failedRecipients = new List<string>();
        string? lastNotificationId = null;
        string? lastError = null;

        try
        {
            var client = new NotificationClient(_config.ApiKey);

            var personalisation = new Dictionary<string, dynamic>
            {
                { "url", reportUrl }
            };

            foreach (var recipient in recipients)
            {
                try
                {
                    _logger.LogInformation(
                        "Sending cleanse report notification email to {Recipient} using template {TemplateId}",
                        recipient, _config.TemplateId);

                    var response = await client.SendEmailAsync(
                        emailAddress: recipient,
                        templateId: _config.TemplateId,
                        personalisation: personalisation
                    );

                    lastNotificationId = response.id;
                    successfulRecipients.Add(recipient);

                    _logger.LogInformation(
                        "Successfully sent cleanse report notification email. NotificationId: {NotificationId}, Recipient: {Recipient}",
                        response.id, recipient);
                }
                catch (NotifyClientException ex)
                {
                    lastError = ex.Message;
                    failedRecipients.Add(recipient);

                    _logger.LogError(ex,
                        "GOV.UK Notify client error when sending cleanse report notification to {Recipient}",
                        recipient);
                }
            }

            var allRecipients = string.Join("; ", recipients);

            if (failedRecipients.Count == 0)
            {
                return new CleanseReportNotificationResult
                {
                    Success = true,
                    NotificationId = lastNotificationId,
                    Recipient = allRecipients
                };
            }

            if (successfulRecipients.Count > 0)
            {
                // Partial success
                return new CleanseReportNotificationResult
                {
                    Success = true,
                    NotificationId = lastNotificationId,
                    Recipient = allRecipients,
                    Error = $"Failed to send to: {string.Join(", ", failedRecipients)}. Error: {lastError}"
                };
            }

            // All failed
            return new CleanseReportNotificationResult
            {
                Success = false,
                Recipient = allRecipients,
                Error = lastError
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Unexpected error when sending cleanse report notifications");

            return new CleanseReportNotificationResult
            {
                Success = false,
                Recipient = string.Join("; ", recipients),
                Error = ex.Message
            };
        }
    }

    /// <inheritdoc />
    public async Task<CleanseReportNotificationResult> SendTestNotificationAsync(string testEmailAddress, CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(_config.ApiKey))
        {
            _logger.LogWarning("GOV.UK Notify API key is not configured. Cannot send test notification.");
            return new CleanseReportNotificationResult
            {
                Success = false,
                Recipient = testEmailAddress,
                Error = "API key not configured"
            };
        }

        try
        {
            _logger.LogInformation(
                "Sending test notification email to {Recipient} using template {TemplateId}",
                testEmailAddress, _config.TemplateId);

            var client = new NotificationClient(_config.ApiKey);

            var personalisation = new Dictionary<string, dynamic>
            {
                { "url", "https://example.com/test-report.zip" }
            };

            var response = await client.SendEmailAsync(
                emailAddress: testEmailAddress,
                templateId: _config.TemplateId,
                personalisation: personalisation
            );

            _logger.LogInformation(
                "Successfully sent test notification email. NotificationId: {NotificationId}, Recipient: {Recipient}",
                response.id, testEmailAddress);

            return new CleanseReportNotificationResult
            {
                Success = true,
                NotificationId = response.id,
                Recipient = testEmailAddress
            };
        }
        catch (NotifyClientException ex)
        {
            _logger.LogError(ex,
                "GOV.UK Notify client error when sending test notification to {Recipient}. Message: {Message}",
                testEmailAddress, ex.Message);

            return new CleanseReportNotificationResult
            {
                Success = false,
                Recipient = testEmailAddress,
                Error = ex.Message
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Unexpected error when sending test notification to {Recipient}. Message: {Message}",
                testEmailAddress, ex.Message);

            return new CleanseReportNotificationResult
            {
                Success = false,
                Recipient = testEmailAddress,
                Error = ex.Message
            };
        }
    }
}
