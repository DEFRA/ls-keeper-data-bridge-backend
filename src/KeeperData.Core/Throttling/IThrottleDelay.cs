namespace KeeperData.Core.Throttling;

/// <summary>
/// Abstraction over Task.Delay for throttling, enabling testability.
/// </summary>
public interface IThrottleDelay
{
    Task DelayAsync(int milliseconds, CancellationToken ct);
}
