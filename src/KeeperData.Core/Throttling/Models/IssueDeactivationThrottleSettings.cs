using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Throttling.Models;

[ExcludeFromCodeCoverage]
public sealed record IssueDeactivationThrottleSettings
{
    public int BatchSize { get; init; } = Defaults.BatchSize;
    public int ThrottleDelayMs { get; init; } = Defaults.ThrottleDelayMs;
    public int RpmWindowSeconds { get; init; } = Defaults.RpmWindowSeconds;

    public static class Defaults
    {
        public const int BatchSize = 200;
        public const int ThrottleDelayMs = 500;
        public const int RpmWindowSeconds = 30;
    }
}
