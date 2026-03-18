using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Throttling.Models;

[ExcludeFromCodeCoverage]
public sealed record IssueQueryThrottleSettings
{
    public int StreamBatchSize { get; init; } = Defaults.StreamBatchSize;

    public static class Defaults
    {
        public const int StreamBatchSize = 500;
    }
}
