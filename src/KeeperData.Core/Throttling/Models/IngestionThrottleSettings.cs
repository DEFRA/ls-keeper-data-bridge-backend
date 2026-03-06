using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Throttling.Models;

[ExcludeFromCodeCoverage]
public sealed record IngestionThrottleSettings
{
    public int BatchSize { get; init; } = Defaults.BatchSize;
    public int BatchDelayMs { get; init; } = Defaults.BatchDelayMs;
    public int ProgressUpdateInterval { get; init; } = Defaults.ProgressUpdateInterval;
    public int LogInterval { get; init; } = Defaults.LogInterval;

    public static class Defaults
    {
        public const int BatchSize = 100;
        public const int BatchDelayMs = 1000;
        public const int ProgressUpdateInterval = 100;
        public const int LogInterval = 100;
    }
}
