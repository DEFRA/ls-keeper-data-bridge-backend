using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Impl;

[ExcludeFromCodeCoverage]
public sealed class ZeroThrottler : IThrottler
{
    public const string PolicyName = "Zero (Unthrottled)";
    public const string PolicySlug = "zero";

    public static ThrottlePolicySettings ZeroSettings { get; } = new()
    {
        Ingestion = new()
        {
            BatchSize = 5000,
            BatchDelayMs = 0,
            ProgressUpdateInterval = 100,
            LogInterval = 100
        },
        CleanseAnalysis = new()
        {
            PumpBatchSize = 2000,
            PumpDelayMs = 0,
            RecordIssueDelayMs = 0,
            ProgressUpdateInterval = 50,
            RpmWindowSeconds = 60
        },
        CleanseExport = new()
        {
            StreamBatchSize = 5000,
            ThrottlingDelayMs = 0,
            RpmWindowSeconds = 30
        },
        IssueDeactivation = new()
        {
            BatchSize = 5000,
            ThrottleDelayMs = 0,
            RpmWindowSeconds = 30
        },
        IssueQuery = new()
        {
            StreamBatchSize = 5000
        }
    };

    public ThrottlePolicySettings Settings => ZeroSettings;
    public string ActivePolicyName => PolicyName;
    public string ActivePolicySlug => PolicySlug;

    public Task DelayAsync(int milliseconds, CancellationToken ct)
        => Task.CompletedTask;
}
