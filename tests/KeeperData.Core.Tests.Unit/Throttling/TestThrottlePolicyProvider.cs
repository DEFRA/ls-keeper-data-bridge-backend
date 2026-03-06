using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Tests.Unit.Throttling;

public sealed class TestThrottlePolicyProvider : IThrottlePolicyProvider
{
    public ThrottlePolicySettings Current { get; set; } = UnitTestSettings;
    public string ActivePolicyName { get; set; } = "UnitTest";
    public string ActivePolicySlug { get; set; } = "unit-test";

    public void Refresh(ThrottlePolicy? activePolicy)
    {
        if (activePolicy is not null)
        {
            Current = activePolicy.Settings;
            ActivePolicyName = activePolicy.Name;
            ActivePolicySlug = activePolicy.Slug;
        }
    }

    public static ThrottlePolicySettings UnitTestSettings { get; } = new()
    {
        Ingestion = new() { BatchSize = 1000, BatchDelayMs = 0, ProgressUpdateInterval = 1000, LogInterval = 1000 },
        CleanseAnalysis = new() { PumpBatchSize = 500, PumpDelayMs = 0, RecordIssueDelayMs = 0, ProgressUpdateInterval = 500 },
        CleanseExport = new() { StreamBatchSize = 5000, ThrottlingDelayMs = 0 },
        IssueDeactivation = new() { BatchSize = 2000, ThrottleDelayMs = 0 },
        IssueQuery = new() { StreamBatchSize = 5000 }
    };
}
