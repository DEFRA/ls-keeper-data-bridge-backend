using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Throttling.Models;

[ExcludeFromCodeCoverage]
public sealed record CleanseExportThrottleSettings
{
    public int StreamBatchSize { get; init; } = Defaults.StreamBatchSize;
    public int ThrottlingDelayMs { get; init; } = Defaults.ThrottlingDelayMs;

    public static class Defaults
    {
        public const int StreamBatchSize = 500;
        public const int ThrottlingDelayMs = 250;
    }
}
