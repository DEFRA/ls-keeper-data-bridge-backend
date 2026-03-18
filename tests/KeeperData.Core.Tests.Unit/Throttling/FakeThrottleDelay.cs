using KeeperData.Core.Throttling;

namespace KeeperData.Core.Tests.Unit.Throttling;

public sealed class FakeThrottleDelay : IThrottleDelay
{
    private readonly List<int> _invocations = [];

    public IReadOnlyList<int> Invocations => _invocations;
    public int TotalInvocations => _invocations.Count;

    public Task DelayAsync(int milliseconds, CancellationToken ct)
    {
        _invocations.Add(milliseconds);
        return Task.CompletedTask;
    }

    public void Reset() => _invocations.Clear();
}
