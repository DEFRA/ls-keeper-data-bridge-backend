namespace KeeperData.Infrastructure.Messaging.Configuration;

public class ServiceBusSenderConfiguration : IServiceBusSenderConfiguration
{
    public TopicConfiguration DataBridgeEventsTopic { get; init; } = new();
}