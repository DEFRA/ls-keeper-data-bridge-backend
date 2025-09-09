namespace KeeperData.Infrastructure.Messaging.Configuration;

public interface IServiceBusSenderConfiguration
{
    TopicConfiguration DataBridgeEventsTopic { get; init; }
}