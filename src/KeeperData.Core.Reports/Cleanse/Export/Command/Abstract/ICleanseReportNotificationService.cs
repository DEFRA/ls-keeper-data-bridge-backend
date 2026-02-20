using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;

namespace KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;

/// <summary>
/// Service for sending cleanse report notification emails.
/// </summary>
public interface ICleanseReportNotificationService
{
    /// <summary>
    /// Sends an email notification with a link to download the cleanse report.
    /// </summary>
    /// <param name="reportUrl">The presigned URL to download the report.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The result of the notification attempt.</returns>
    Task<CleanseReportNotificationResult> SendReportNotificationAsync(string reportUrl, CancellationToken ct = default);

    /// <summary>
    /// Sends a test notification email to a specified email address.
    /// </summary>
    /// <param name="testEmailAddress">The email address to send the test to.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The result of the test notification attempt.</returns>
    Task<CleanseReportNotificationResult> SendTestNotificationAsync(string testEmailAddress, CancellationToken ct = default);
}
