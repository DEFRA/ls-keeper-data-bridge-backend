using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Throttling.Models;

[ExcludeFromCodeCoverage]
public sealed record ThrottlePolicySettings
{
    public IngestionThrottleSettings Ingestion { get; init; } = new();
    public CleanseAnalysisThrottleSettings CleanseAnalysis { get; init; } = new();
    public CleanseExportThrottleSettings CleanseExport { get; init; } = new();
    public IssueDeactivationThrottleSettings IssueDeactivation { get; init; } = new();
    public IssueQueryThrottleSettings IssueQuery { get; init; } = new();
}
