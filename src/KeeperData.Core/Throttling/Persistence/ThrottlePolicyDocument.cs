using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Throttling.Models;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Throttling.Persistence;

[ExcludeFromCodeCoverage]
[BsonIgnoreExtraElements]
public sealed class ThrottlePolicyDocument
{
    [BsonId]
    public string Slug { get; set; } = string.Empty;

    [BsonElement("name")]
    public string Name { get; set; } = string.Empty;

    [BsonElement("is_active")]
    public bool IsActive { get; set; }

    [BsonElement("settings")]
    public ThrottlePolicySettingsDocument Settings { get; set; } = new();

    [BsonElement("created_at_utc")]
    public DateTime CreatedAtUtc { get; set; }

    [BsonElement("updated_at_utc")]
    public DateTime UpdatedAtUtc { get; set; }

    public ThrottlePolicy ToModel() => new()
    {
        Slug = Slug,
        Name = Name,
        IsActive = IsActive,
        IsReadOnly = false,
        Settings = Settings.ToModel(),
        CreatedAtUtc = CreatedAtUtc,
        UpdatedAtUtc = UpdatedAtUtc
    };

    public static ThrottlePolicyDocument FromModel(ThrottlePolicy policy) => new()
    {
        Slug = policy.Slug,
        Name = policy.Name,
        IsActive = policy.IsActive,
        Settings = ThrottlePolicySettingsDocument.FromModel(policy.Settings),
        CreatedAtUtc = policy.CreatedAtUtc,
        UpdatedAtUtc = policy.UpdatedAtUtc
    };
}

[ExcludeFromCodeCoverage]
[BsonIgnoreExtraElements]
public sealed class ThrottlePolicySettingsDocument
{
    [BsonElement("ingestion")]
    public IngestionSettingsDoc Ingestion { get; set; } = new();

    [BsonElement("cleanse_analysis")]
    public CleanseAnalysisSettingsDoc CleanseAnalysis { get; set; } = new();

    [BsonElement("cleanse_export")]
    public CleanseExportSettingsDoc CleanseExport { get; set; } = new();

    [BsonElement("issue_deactivation")]
    public IssueDeactivationSettingsDoc IssueDeactivation { get; set; } = new();

    [BsonElement("issue_query")]
    public IssueQuerySettingsDoc IssueQuery { get; set; } = new();

    public ThrottlePolicySettings ToModel() => new()
    {
        Ingestion = new() { BatchSize = Ingestion.BatchSize, BatchDelayMs = Ingestion.BatchDelayMs, ProgressUpdateInterval = Ingestion.ProgressUpdateInterval, LogInterval = Ingestion.LogInterval },
        CleanseAnalysis = new() { PumpBatchSize = CleanseAnalysis.PumpBatchSize, PumpDelayMs = CleanseAnalysis.PumpDelayMs, RecordIssueDelayMs = CleanseAnalysis.RecordIssueDelayMs, ProgressUpdateInterval = CleanseAnalysis.ProgressUpdateInterval },
        CleanseExport = new() { StreamBatchSize = CleanseExport.StreamBatchSize, ThrottlingDelayMs = CleanseExport.ThrottlingDelayMs },
        IssueDeactivation = new() { BatchSize = IssueDeactivation.BatchSize, ThrottleDelayMs = IssueDeactivation.ThrottleDelayMs },
        IssueQuery = new() { StreamBatchSize = IssueQuery.StreamBatchSize }
    };

    public static ThrottlePolicySettingsDocument FromModel(ThrottlePolicySettings s) => new()
    {
        Ingestion = new() { BatchSize = s.Ingestion.BatchSize, BatchDelayMs = s.Ingestion.BatchDelayMs, ProgressUpdateInterval = s.Ingestion.ProgressUpdateInterval, LogInterval = s.Ingestion.LogInterval },
        CleanseAnalysis = new() { PumpBatchSize = s.CleanseAnalysis.PumpBatchSize, PumpDelayMs = s.CleanseAnalysis.PumpDelayMs, RecordIssueDelayMs = s.CleanseAnalysis.RecordIssueDelayMs, ProgressUpdateInterval = s.CleanseAnalysis.ProgressUpdateInterval },
        CleanseExport = new() { StreamBatchSize = s.CleanseExport.StreamBatchSize, ThrottlingDelayMs = s.CleanseExport.ThrottlingDelayMs },
        IssueDeactivation = new() { BatchSize = s.IssueDeactivation.BatchSize, ThrottleDelayMs = s.IssueDeactivation.ThrottleDelayMs },
        IssueQuery = new() { StreamBatchSize = s.IssueQuery.StreamBatchSize }
    };
}

[ExcludeFromCodeCoverage]
[BsonIgnoreExtraElements]
public sealed class IngestionSettingsDoc
{
    [BsonElement("batch_size")] public int BatchSize { get; set; } = IngestionThrottleSettings.Defaults.BatchSize;
    [BsonElement("batch_delay_ms")] public int BatchDelayMs { get; set; } = IngestionThrottleSettings.Defaults.BatchDelayMs;
    [BsonElement("progress_update_interval")] public int ProgressUpdateInterval { get; set; } = IngestionThrottleSettings.Defaults.ProgressUpdateInterval;
    [BsonElement("log_interval")] public int LogInterval { get; set; } = IngestionThrottleSettings.Defaults.LogInterval;
}

[ExcludeFromCodeCoverage]
[BsonIgnoreExtraElements]
public sealed class CleanseAnalysisSettingsDoc
{
    [BsonElement("pump_batch_size")] public int PumpBatchSize { get; set; } = CleanseAnalysisThrottleSettings.Defaults.PumpBatchSize;
    [BsonElement("pump_delay_ms")] public int PumpDelayMs { get; set; } = CleanseAnalysisThrottleSettings.Defaults.PumpDelayMs;
    [BsonElement("record_issue_delay_ms")] public int RecordIssueDelayMs { get; set; } = CleanseAnalysisThrottleSettings.Defaults.RecordIssueDelayMs;
    [BsonElement("progress_update_interval")] public int ProgressUpdateInterval { get; set; } = CleanseAnalysisThrottleSettings.Defaults.ProgressUpdateInterval;
}

[ExcludeFromCodeCoverage]
[BsonIgnoreExtraElements]
public sealed class CleanseExportSettingsDoc
{
    [BsonElement("stream_batch_size")] public int StreamBatchSize { get; set; } = CleanseExportThrottleSettings.Defaults.StreamBatchSize;
    [BsonElement("throttling_delay_ms")] public int ThrottlingDelayMs { get; set; } = CleanseExportThrottleSettings.Defaults.ThrottlingDelayMs;
}

[ExcludeFromCodeCoverage]
[BsonIgnoreExtraElements]
public sealed class IssueDeactivationSettingsDoc
{
    [BsonElement("batch_size")] public int BatchSize { get; set; } = IssueDeactivationThrottleSettings.Defaults.BatchSize;
    [BsonElement("throttle_delay_ms")] public int ThrottleDelayMs { get; set; } = IssueDeactivationThrottleSettings.Defaults.ThrottleDelayMs;
}

[ExcludeFromCodeCoverage]
[BsonIgnoreExtraElements]
public sealed class IssueQuerySettingsDoc
{
    [BsonElement("stream_batch_size")] public int StreamBatchSize { get; set; } = IssueQueryThrottleSettings.Defaults.StreamBatchSize;
}
