using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Export.Command.Domain;

/// <summary>
/// Result of sending a cleanse report notification email.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record CleanseReportNotificationResult
{
    /// <summary>
    /// Gets whether the notification was sent successfully.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Gets the notification ID from the email provider.
    /// </summary>
    public string? NotificationId { get; init; }

    /// <summary>
    /// Gets the recipient email address.
    /// </summary>
    public string? Recipient { get; init; }

    /// <summary>
    /// Gets an error message if the notification failed.
    /// </summary>
    public string? Error { get; init; }
}
