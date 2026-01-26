namespace KeeperData.Infrastructure.Notifications.Configuration;

/// <summary>
/// Configuration for GOV.UK Notify cleanse report email notifications.
/// </summary>
public class CleanseReportNotificationConfig
{
    /// <summary>
    /// The configuration section name.
    /// </summary>
    public const string SectionName = "CleanseReportNotification";

    /// <summary>
    /// Gets or sets the GOV.UK Notify API key.
    /// </summary>
    public string ApiKey { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the GOV.UK Notify template ID for the cleanse report email.
    /// </summary>
    public string TemplateId { get; set; } = "228237fc-a48b-4fda-8a88-34b724dfe3e7";

    /// <summary>
    /// Gets or sets the recipient email addresses for cleanse report notifications.
    /// Supports multiple recipients separated by comma (,) or semicolon (;).
    /// </summary>
    public string RecipientEmails { get; set; } = "kris.dyson@defra.gov.uk";

    /// <summary>
    /// Gets or sets whether notifications are enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Gets the list of recipient email addresses parsed from <see cref="RecipientEmails"/>.
    /// </summary>
    public IReadOnlyList<string> GetRecipientEmailList()
    {
        if (string.IsNullOrWhiteSpace(RecipientEmails))
            return [];

        return RecipientEmails
            .Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(email => !string.IsNullOrWhiteSpace(email))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    /// <summary>
    /// Gets a masked version of the recipient emails for display (first 3 characters of each).
    /// </summary>
    public IReadOnlyList<string> GetMaskedRecipientEmails()
    {
        return GetRecipientEmailList()
            .Select(email => email.Length >= 3 ? email[..3] + "***" : "***")
            .ToList();
    }
}
