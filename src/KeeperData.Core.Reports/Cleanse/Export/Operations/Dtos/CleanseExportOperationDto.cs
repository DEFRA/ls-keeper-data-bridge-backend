using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace KeeperData.Core.Reports.Cleanse.Export.Operations.Dtos;

/// <summary>
/// Full DTO for an ad-hoc cleanse export operation, including progress and report details.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO class - no logic to test.")]
public class CleanseExportOperationDto
{
    public required string Id { get; set; }
    public required string Status { get; set; }
    public DateTime StartedAtUtc { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? CompletedAtUtc { get; set; }

    public double ProgressPercentage { get; set; }
    public string StatusDescription { get; set; } = string.Empty;
    public int TotalRecords { get; set; }
    public int RecordsExported { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ReportObjectKey { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ReportUrl { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Error { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public long? DurationMs { get; set; }
}
