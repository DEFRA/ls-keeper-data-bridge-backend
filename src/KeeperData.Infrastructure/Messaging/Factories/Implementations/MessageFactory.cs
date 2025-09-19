using Amazon.SimpleNotificationService.Model;
using KeeperData.Core.Messaging.Extensions;
using System.Text.Json;

namespace KeeperData.Infrastructure.Messaging.Factories.Implementations;

public class MessageFactory : IMessageFactory
{
    private const string EventTimeUtc = "EventTimeUtc";

    public PublishRequest CreateMessage<TBody>(
        string topicArn,
        TBody body,
        string? subject = null,
        Dictionary<string, string>? additionalUserProperties = null)
    {
        var messageType = typeof(TBody).Name;

        return GenerateMessage(
            topicArn,
            SerializeToJson(body),
            subject ?? messageType,
            additionalUserProperties);
    }

    private static PublishRequest GenerateMessage(
        string topicArn,
        string body,
        string subject,
        Dictionary<string, string>? additionalUserProperties)
    {
        var dateTime = DateTime.UtcNow;

        var message = new PublishRequest(topicArn, body, subject)
        {
            MessageAttributes = []
        };

        message.MessageAttributes.Add(EventTimeUtc, new MessageAttributeValue
        {
            DataType = "String",
            StringValue = dateTime.ToString()
        });

        foreach (var (key, value) in additionalUserProperties ?? [])
        {
            message.MessageAttributes.Add(key, new MessageAttributeValue
            {
                DataType = "String",
                StringValue = value
            });
        }

        message.MessageAttributes.TryAdd("Subject", new MessageAttributeValue
        {
            DataType = "String",
            StringValue = subject.ReplaceSuffix()
        });

        // TODO - Write this out with a Logical Context AsyncLocal implementation
        message.MessageAttributes.TryAdd("CorrelationId", new MessageAttributeValue
        {
            DataType = "String",
            StringValue = Guid.NewGuid().ToString()
        });

        return message;
    }

    private static string SerializeToJson<TBody>(TBody value)
    {
        return typeof(TBody) switch
        {
            // Add specific 'Source Generations' here for message types
            _ => JsonSerializer.Serialize(value, JsonDefaults.DefaultOptionsWithStringEnumConversion)
        };
    }
}