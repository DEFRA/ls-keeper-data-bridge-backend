using FluentAssertions;
using KeeperData.Infrastructure.Messaging.Clients;

namespace KeeperData.Infrastructure.Tests.Unit.Messaging.Clients;

public class DataBridgeTopicClientTests
{
    [Fact]
    public void ClientName_ReturnsClassName()
    {
        var client = new DataBridgeTopicClient();

        var result = client.ClientName;

        result.Should().Be(nameof(DataBridgeTopicClient));
    }

}