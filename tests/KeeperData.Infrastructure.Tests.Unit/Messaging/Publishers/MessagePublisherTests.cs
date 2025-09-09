using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using FluentAssertions;
using KeeperData.Infrastructure.Messaging.Configuration;
using KeeperData.Infrastructure.Messaging.Exceptions;
using KeeperData.Infrastructure.Messaging.Factories.Implementations;
using KeeperData.Infrastructure.Messaging.Publishers.Implementations;
using Moq;
using System.Net;

namespace KeeperData.Infrastructure.Tests.Unit.Messaging.Publishers;

public class MessagePublisherTests
{
    private readonly Mock<IAmazonSimpleNotificationService> _amazonSimpleNotificationServiceMock = new();
    private readonly Mock<IServiceBusSenderConfiguration> _serviceBusSenderConfigurationMock = new();
    private readonly MessageFactory _messageFactory = new();

    private readonly DataBridgeMessagePublisher _sut;

    private const string TestTopicArn = "arn:aws:sns:eu-west-2:000000000000:test-topic";

    public MessagePublisherTests()
    {
        _amazonSimpleNotificationServiceMock
            .Setup(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PublishResponse { HttpStatusCode = HttpStatusCode.OK });

        SetupServiceBusSenderConfiguration(TestTopicArn);

        _sut = new DataBridgeMessagePublisher(_amazonSimpleNotificationServiceMock.Object, _messageFactory, _serviceBusSenderConfigurationMock.Object);
    }

    [Fact]
    public async Task GivenValidMessage_WhenCallingPublishAsync_ShouldSucceed()
    {
        var testMessage = new TestPublishMessage { Id = Guid.NewGuid(), Name = Guid.NewGuid().ToString() };

        Func<Task> func = async () => await _sut.PublishAsync(testMessage, CancellationToken.None);

        await func.Should().NotThrowAsync();
    }

    [Fact]
    public async Task GivenNullMessage_WhenCallingPublishAsync_ShouldThrow()
    {
        Func<Task> func = async () => await _sut.PublishAsync<TestPublishMessage>(null, CancellationToken.None);

        await func.Should().ThrowAsync<ArgumentException>().WithMessage("Message payload was null (Parameter 'message')");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task GivenMissingTopicArn_WhenCallingPublishAsync_ShouldThrow(string? topicArn)
    {
        SetupServiceBusSenderConfiguration(topicArn);

        var testMessage = new { Id = Guid.NewGuid(), Name = Guid.NewGuid().ToString() };

        Func<Task> func = async () => await _sut.PublishAsync(testMessage, CancellationToken.None);

        await func.Should().ThrowAsync<PublishFailedException>().WithMessage("TopicArn is missing");
    }

    [Fact]
    public async Task GivenValidMessage_AndSnsServiceFails_WhenCallingPublishAsync_ShouldThrow()
    {
        _amazonSimpleNotificationServiceMock
            .Setup(x => x.PublishAsync(It.IsAny<PublishRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NotFoundException("Topic not found"));

        var testMessage = new { Id = Guid.NewGuid(), Name = Guid.NewGuid().ToString() };

        Func<Task> func = async () => await _sut.PublishAsync(testMessage, CancellationToken.None);

        var exceptionAssertion = await func.Should().ThrowAsync<PublishFailedException>();
        exceptionAssertion.And.Message.Should().Be($"Failed to publish event on {TestTopicArn}.");
        exceptionAssertion.And.InnerException.Should().BeOfType<NotFoundException>();
        exceptionAssertion.And.InnerException!.Message.Should().Be("Topic not found");
    }

    private void SetupServiceBusSenderConfiguration(string? topicArn)
    {
        _serviceBusSenderConfigurationMock.Setup(c => c.DataBridgeEventsTopic).Returns(new TopicConfiguration
        {
            TopicArn = topicArn!
        });
    }
}

public class TestPublishMessage
{
    public Guid Id { get; set; }
    public string? Name { get; set; } = string.Empty;
}