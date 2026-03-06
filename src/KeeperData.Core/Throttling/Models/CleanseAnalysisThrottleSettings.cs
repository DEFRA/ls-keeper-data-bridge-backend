using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Throttling.Models;

[ExcludeFromCodeCoverage]
public sealed record CleanseAnalysisThrottleSettings
{
    public int PumpBatchSize { get; init; } = Defaults.PumpBatchSize;
    public int PumpDelayMs { get; init; } = Defaults.PumpDelayMs;
    public int RecordIssueDelayMs { get; init; } = Defaults.RecordIssueDelayMs;
    public int ProgressUpdateInterval { get; init; } = Defaults.ProgressUpdateInterval;
    public int RpmWindowSeconds { get; init; } = Defaults.RpmWindowSeconds;

    public static class Defaults
    {
        public const int PumpBatchSize = 50;
        public const int PumpDelayMs = 300;
        public const int RecordIssueDelayMs = 100;
        public const int ProgressUpdateInterval = 50;
        public const int RpmWindowSeconds = 60;
    }
}
