namespace KeeperData.Core.ETL.Models;

public enum ExportStatusType
{
    Queued,
    Running,
    Succeeded,
    Failed
}

public record CphExportStatus
{
    public required Guid ExportId { get; init; }
    public required ExportStatusType Status { get; set; }
    public required DateTime RequestedAt { get; init; }
    public DateTime? StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public required string SourceDuckDbPath { get; init; }
    public string? SqlitePath { get; set; }
    public int? RowCount { get; set; }
    public string? ErrorMessage { get; set; }
}
