using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Throttling.Models;

[ExcludeFromCodeCoverage]
public sealed record IssueDeactivationThrottleSettings
{
    public int BatchSize { get; init; } = Defaults.BatchSize;
    public int ThrottleDelayMs { get; init; } = Defaults.ThrottleDelayMs;

    public static class Defaults
    {
        public const int BatchSize = 200;
        public const int ThrottleDelayMs = 500;
    }
}
