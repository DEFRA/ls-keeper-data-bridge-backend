using FluentAssertions;
using KeeperData.Infrastructure.Messaging.Configuration;

namespace KeeperData.Infrastructure.Tests.Unit.Messaging.Configuration;

public class TopicConfigurationTests
{
    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var config = new TopicConfiguration();

        config.HealthcheckEnabled.Should().BeFalse();
        config.TopicName.Should().BeEmpty();
        config.TopicArn.Should().BeEmpty();
    }

    [Fact]
    public void CustomValues_CanBeSet()
    {
        var config = new TopicConfiguration
        {
            HealthcheckEnabled = true,
            TopicName = "my-topic",
            TopicArn = "arn:aws:sns:eu-west-2:123456789:my-topic"
        };

        config.HealthcheckEnabled.Should().BeTrue();
        config.TopicName.Should().Be("my-topic");
        config.TopicArn.Should().Be("arn:aws:sns:eu-west-2:123456789:my-topic");
    }

    [Fact]
    public void Record_SupportsWithExpression()
    {
        var original = new TopicConfiguration { TopicName = "original" };
        var modified = original with { TopicName = "modified" };

        modified.TopicName.Should().Be("modified");
        original.TopicName.Should().Be("original");
    }

    [Fact]
    public void Record_SupportsEquality()
    {
        var config1 = new TopicConfiguration { TopicName = "test", HealthcheckEnabled = true };
        var config2 = new TopicConfiguration { TopicName = "test", HealthcheckEnabled = true };
        var config3 = new TopicConfiguration { TopicName = "other", HealthcheckEnabled = true };

        config1.Should().Be(config2);
        config1.Should().NotBe(config3);
    }
}
