using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling;

[ExcludeFromCodeCoverage]
public static partial class ThrottlePolicyDefaults
{
    public const string NormalSlug = "normal";
    public const string NormalName = "Normal";

    public static ThrottlePolicy NormalPolicy { get; } = new()
    {
        Slug = NormalSlug,
        Name = NormalName,
        IsActive = false,
        IsReadOnly = true,
        Settings = new ThrottlePolicySettings(),
        CreatedAtUtc = DateTime.MinValue,
        UpdatedAtUtc = DateTime.MinValue
    };

    public static IReadOnlyList<ThrottlePolicy> SeedPolicies { get; } =
    [
        new()
        {
            Slug = ToSlug("L1 Light Throttle"),
            Name = "L1 Light Throttle",
            Settings = new ThrottlePolicySettings
            {
                Ingestion = new() { BatchSize = 500, BatchDelayMs = 200, ProgressUpdateInterval = 500, LogInterval = 500 },
                CleanseAnalysis = new() { PumpBatchSize = 200, PumpDelayMs = 50, RecordIssueDelayMs = 20, ProgressUpdateInterval = 200 },
                CleanseExport = new() { StreamBatchSize = 2000, ThrottlingDelayMs = 50 },
                IssueDeactivation = new() { BatchSize = 1000, ThrottleDelayMs = 100 },
                IssueQuery = new() { StreamBatchSize = 2000 }
            }
        },
        new()
        {
            Slug = ToSlug("L2 Moderate Throttle"),
            Name = "L2 Moderate Throttle",
            Settings = new ThrottlePolicySettings
            {
                Ingestion = new() { BatchSize = 200, BatchDelayMs = 500, ProgressUpdateInterval = 200, LogInterval = 200 },
                CleanseAnalysis = new() { PumpBatchSize = 100, PumpDelayMs = 150, RecordIssueDelayMs = 50, ProgressUpdateInterval = 100 },
                CleanseExport = new() { StreamBatchSize = 1000, ThrottlingDelayMs = 100 },
                IssueDeactivation = new() { BatchSize = 500, ThrottleDelayMs = 200 },
                IssueQuery = new() { StreamBatchSize = 1000 }
            }
        },
        new()
        {
            Slug = ToSlug("L3 Heavy Throttle"),
            Name = "L3 Heavy Throttle",
            Settings = new ThrottlePolicySettings
            {
                Ingestion = new() { BatchSize = 50, BatchDelayMs = 2000, ProgressUpdateInterval = 50, LogInterval = 50 },
                CleanseAnalysis = new() { PumpBatchSize = 25, PumpDelayMs = 500, RecordIssueDelayMs = 200, ProgressUpdateInterval = 25 },
                CleanseExport = new() { StreamBatchSize = 250, ThrottlingDelayMs = 500 },
                IssueDeactivation = new() { BatchSize = 100, ThrottleDelayMs = 1000 },
                IssueQuery = new() { StreamBatchSize = 250 }
            }
        }
    ];

    public static string ToSlug(string name)
    {
        var slug = name.ToLowerInvariant().Trim();
        slug = SlugRegex().Replace(slug, "-");
        slug = MultiHyphenRegex().Replace(slug, "-");
        return slug.Trim('-');
    }

    [GeneratedRegex(@"[^a-z0-9\-]")]
    private static partial Regex SlugRegex();

    [GeneratedRegex(@"-{2,}")]
    private static partial Regex MultiHyphenRegex();
}
