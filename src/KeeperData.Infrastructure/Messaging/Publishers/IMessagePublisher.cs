namespace KeeperData.Infrastructure.Messaging.Publishers;

public interface IMessagePublisher<in T> where T : ITopicClient, new()
{
    string TopicArn { get; }

    Task PublishAsync<TMessage>(TMessage? message, CancellationToken cancellationToken = default);
}