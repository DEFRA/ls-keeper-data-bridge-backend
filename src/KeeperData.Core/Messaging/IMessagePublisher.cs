namespace KeeperData.Core.Messaging;

public interface IMessagePublisher<in T> where T : ITopicClient, new()
{
    string TopicArn { get; }

    Task PublishAsync<TMessage>(TMessage? message, CancellationToken cancellationToken = default);
}