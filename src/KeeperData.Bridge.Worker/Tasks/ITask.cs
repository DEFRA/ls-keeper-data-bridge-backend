namespace KeeperData.Bridge.Worker.Tasks;

public interface ITask
{
    Task RunAsync(CancellationToken cancellationToken);
}