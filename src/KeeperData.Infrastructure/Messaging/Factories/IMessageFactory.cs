using Amazon.SimpleNotificationService.Model;

namespace KeeperData.Infrastructure.Messaging.Factories;

public interface IMessageFactory
{
    PublishRequest CreateMessage<TBody>(
        string topicArn,
        TBody body,
        string? subject = null,
        Dictionary<string, string>? additionalUserProperties = null);
}