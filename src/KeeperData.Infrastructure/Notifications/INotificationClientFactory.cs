using Notify.Client;

namespace KeeperData.Infrastructure.Notifications;

/// <summary>
/// Factory for creating GOV.UK Notify clients with appropriate configuration.
/// </summary>
public interface INotificationClientFactory
{
    /// <summary>
    /// Creates a new NotificationClient instance configured with proxy settings if available.
    /// </summary>
    /// <param name="apiKey">The GOV.UK Notify API key.</param>
    /// <returns>A configured NotificationClient instance.</returns>
    NotificationClient Create(string apiKey);
}
