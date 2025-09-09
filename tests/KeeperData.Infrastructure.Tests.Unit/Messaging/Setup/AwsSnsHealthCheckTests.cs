using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using FluentAssertions;
using KeeperData.Infrastructure.Messaging.Configuration;
using KeeperData.Infrastructure.Messaging.Setup;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Moq;
using System.Net;

namespace KeeperData.Infrastructure.Tests.Unit.Messaging.Setup;

public class AwsSnsHealthCheckTests
{
    private readonly Mock<IAmazonSimpleNotificationService> _amazonSimpleNotificationServiceMock;
    private readonly Mock<IServiceBusSenderConfiguration> _serviceBusSenderConfigurationMock;
    private readonly HealthCheckContext _healthCheckContext = new();

    private readonly AwsSnsHealthCheck _sut;

    private const string DataBridgeEventsTopicName = "ls-keeper-data-bridge-events";
    private const string DataBridgeEventsTopicArn = $"arn:aws:sns:eu-west-2:000000000000:{DataBridgeEventsTopicName}";

    public AwsSnsHealthCheckTests()
    {
        _amazonSimpleNotificationServiceMock = new Mock<IAmazonSimpleNotificationService>();

        _amazonSimpleNotificationServiceMock
            .Setup(x => x.ListTopicsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListTopicsResponse { HttpStatusCode = HttpStatusCode.OK, Topics = [new Topic { TopicArn = DataBridgeEventsTopicArn }] });

        _amazonSimpleNotificationServiceMock
            .Setup(x => x.GetTopicAttributesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetTopicAttributesResponse { HttpStatusCode = HttpStatusCode.OK });

        _serviceBusSenderConfigurationMock = new Mock<IServiceBusSenderConfiguration>();
        SetupServiceBusSenderConfiguration(DataBridgeEventsTopicName, DataBridgeEventsTopicArn);

        _sut = new AwsSnsHealthCheck(_amazonSimpleNotificationServiceMock.Object, _serviceBusSenderConfigurationMock.Object);
    }

    [Fact]
    public async Task GivenValidTopicName_WhenCallingCheckHealthAsync_ShouldSucceed()
    {
        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task GivenListTopicsRequestFails_WhenCallingCheckHealthAsync_ShouldFail()
    {
        SetupServiceBusSenderConfiguration(DataBridgeEventsTopicName, string.Empty);

        _amazonSimpleNotificationServiceMock
            .Setup(x => x.ListTopicsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListTopicsResponse { HttpStatusCode = HttpStatusCode.InternalServerError });

        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Degraded);
        result.Description.Should().Be("SNS ListTopics returned non-OK status.");
    }

    [Fact]
    public async Task GivenInValidTopicName_WhenCallingCheckHealthAsync_ShouldFail()
    {
        SetupServiceBusSenderConfiguration("dummy-topic-name", string.Empty);

        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Be($"SNS topic 'dummy-topic-name' not found.");
    }

    [Fact]
    public async Task GivenGetTopicAttributesAsyncRequestFails_WhenCallingCheckHealthAsync_ShouldFail()
    {
        SetupServiceBusSenderConfiguration(DataBridgeEventsTopicName, string.Empty);

        _amazonSimpleNotificationServiceMock
            .Setup(x => x.GetTopicAttributesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetTopicAttributesResponse { HttpStatusCode = HttpStatusCode.InternalServerError });

        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Degraded);
        result.Description.Should().Be($"SNS topic '{DataBridgeEventsTopicName}' attributes fetch returned non-OK status.");
    }

    private void SetupServiceBusSenderConfiguration(string topicName, string topicArn = "")
    {
        _serviceBusSenderConfigurationMock.Setup(c => c.DataBridgeEventsTopic).Returns(new TopicConfiguration
        {
            TopicName = topicName,
            TopicArn = topicArn
        });
    }
}