using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Throttling;

[ExcludeFromCodeCoverage(Justification = "Thin wrapper over Task.Delay - verified by a single smoke test.")]
public sealed class ThrottleDelay : IThrottleDelay
{
    public Task DelayAsync(int milliseconds, CancellationToken ct)
    {
        return milliseconds > 0 ? Task.Delay(milliseconds, ct) : Task.CompletedTask;
    }
}
