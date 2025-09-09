namespace KeeperData.Infrastructure.Messaging.Configuration;

public record TopicConfiguration
{
    public bool HealthcheckEnabled { get; init; }
    public string TopicName { get; init; } = string.Empty;
    public string TopicArn { get; init; } = string.Empty;
}