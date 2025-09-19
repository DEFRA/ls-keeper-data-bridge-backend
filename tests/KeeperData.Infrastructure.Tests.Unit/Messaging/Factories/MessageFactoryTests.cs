using FluentAssertions;
using KeeperData.Infrastructure.Messaging.Factories.Implementations;

namespace KeeperData.Infrastructure.Tests.Unit.Messaging.Factories;

public class MessageFactoryTests
{
    private readonly MessageFactory _factory = new();

    private const string TestTopicArn = "arn:aws:sns:eu-west-2:000000000000:test-topic";

    [Fact]
    public void GivenTestMessage_WhenCallingCreateMessage_ShouldSerializeBodyAndSetSubjectToTypeName()
    {
        var testMessage = new TestMessage { Id = Guid.NewGuid(), Name = Guid.NewGuid().ToString() };

        var result = _factory.CreateMessage(TestTopicArn, testMessage);

        result.TopicArn.Should().Be(TestTopicArn);
        result.Subject.Should().Be("TestMessage");

        result.Message.Should().Contain($"\"id\":\"{testMessage.Id}\"");
        result.Message.Should().Contain($"\"name\":\"{testMessage.Name}\"");
    }

    [Fact]
    public void GivenCustomSubject_WhenCallingCreateMessage_ShouldUseProvidedSubject()
    {
        var testMessage = new TestMessage { Id = Guid.NewGuid(), Name = Guid.NewGuid().ToString() };

        var result = _factory.CreateMessage(TestTopicArn, testMessage, subject: "CustomSubject");

        result.Subject.Should().Be("CustomSubject");
    }

    [Fact]
    public void GivenNullAdditionalUserProperties_WhenCallingCreateMessage_ShouldNotThrowAndIncludeEventTimeUtc()
    {
        var testMessage = new TestMessage { Id = Guid.NewGuid(), Name = Guid.NewGuid().ToString() };

        var result = _factory.CreateMessage(TestTopicArn, testMessage, additionalUserProperties: null);

        result.MessageAttributes.Should().ContainKey("EventTimeUtc");
        result.MessageAttributes.Count.Should().Be(3);
    }

    [Fact]
    public void GivenNoAdditionalProperties_WhenCallingCreateMessage_ShouldIncludeEventTimeUtcOnly()
    {
        var testMessage = new TestMessage { Id = Guid.NewGuid(), Name = Guid.NewGuid().ToString() };

        var result = _factory.CreateMessage(TestTopicArn, testMessage);

        result.MessageAttributes.Should().ContainKey("EventTimeUtc");
        result.MessageAttributes.Count.Should().Be(3);
    }

    [Fact]
    public void GivenAdditionalUserProperties_WhenCallingCreateMessage_ShouldIncludeAllAttributes()
    {
        var testMessage = new TestMessage { Id = Guid.NewGuid(), Name = Guid.NewGuid().ToString() };

        var props = new Dictionary<string, string>
        {
            { "CustomPropertyA", "123" },
            { "CustomPropertyB", "456" }
        };

        var result = _factory.CreateMessage(TestTopicArn, testMessage, additionalUserProperties: props);

        result.MessageAttributes.Should().ContainKey("EventTimeUtc");
        result.MessageAttributes.Should().ContainKey("CustomPropertyA");
        result.MessageAttributes["CustomPropertyA"].StringValue.Should().Be("123");
        result.MessageAttributes.Should().ContainKey("CustomPropertyB");
        result.MessageAttributes["CustomPropertyB"].StringValue.Should().Be("456");
    }
}

public class TestMessage
{
    public Guid Id { get; set; }
    public string? Name { get; set; } = string.Empty;
}