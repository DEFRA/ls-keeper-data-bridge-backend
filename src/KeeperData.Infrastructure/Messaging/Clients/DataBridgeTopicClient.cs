using KeeperData.Core.Messaging;

namespace KeeperData.Infrastructure.Messaging.Clients;

public class DataBridgeTopicClient : ITopicClient
{
    public string ClientName => GetType().Name;
}