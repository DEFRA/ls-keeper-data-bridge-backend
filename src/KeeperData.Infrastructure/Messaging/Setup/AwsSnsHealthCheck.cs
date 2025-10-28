using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using KeeperData.Infrastructure.Messaging.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KeeperData.Infrastructure.Messaging.Setup;

public class AwsSnsHealthCheck(IAmazonSimpleNotificationService snsClient, IServiceBusSenderConfiguration config) : IHealthCheck
{
    private readonly IAmazonSimpleNotificationService _snsClient = snsClient ?? throw new ArgumentNullException(nameof(snsClient));
    private readonly IServiceBusSenderConfiguration _config = config ?? throw new ArgumentNullException(nameof(config));

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var defaultTopicName = _config.DataBridgeEventsTopic.TopicName;
        var defaultTopicArn = _config.DataBridgeEventsTopic.TopicArn ?? string.Empty;

        try
        {
            var result = !string.IsNullOrEmpty(defaultTopicArn) ?
                 await CheckByTopicArn(defaultTopicName, defaultTopicArn, cancellationToken)
                 : await CheckByTopicName(defaultTopicName, cancellationToken);
            return HealthCheckResult.Degraded($"SNS topic '{defaultTopicName}' is probably fine actually, result was '{result}'");
        }
        catch (NotFoundException ex)
        {
            return HealthCheckResult.Unhealthy($"SNS topic '{defaultTopicName}' does not exist.", ex);
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy($"Error accessing SNS topic '{defaultTopicName}'.", ex);
        }
    }

    private async Task<HealthCheckResult> CheckByTopicName(string topicName, CancellationToken cancellationToken = default)
    {
        var listResponse = await _snsClient.ListTopicsAsync(cancellationToken);

        if (listResponse.HttpStatusCode != System.Net.HttpStatusCode.OK)
        {
            return HealthCheckResult.Degraded("SNS ListTopics returned non-OK status.");
        }

        var topic = listResponse.Topics.FirstOrDefault(t => t.TopicArn.EndsWith(topicName, StringComparison.InvariantCultureIgnoreCase));

        if (topic == null || string.IsNullOrWhiteSpace(topic.TopicArn))
        {
            return HealthCheckResult.Unhealthy($"SNS topic '{topicName}' not found.");
        }

        return await CheckByTopicArn(topicName, topic.TopicArn, cancellationToken);
    }

    private async Task<HealthCheckResult> CheckByTopicArn(string topicName, string topicArn, CancellationToken cancellationToken = default)
    {
        var attrResponse = await _snsClient.GetTopicAttributesAsync(topicArn, cancellationToken);

        if (attrResponse.HttpStatusCode != System.Net.HttpStatusCode.OK)
        {
            return HealthCheckResult.Degraded($"SNS topic '{topicName}' attributes fetch returned non-OK status.");
        }

        return HealthCheckResult.Healthy($"SNS topic '{topicName}' is reachable.", new Dictionary<string, object>
        {
            ["TopicArn"] = topicArn
        });
    }
}