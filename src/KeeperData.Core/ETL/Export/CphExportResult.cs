namespace KeeperData.Core.ETL.Export;

public record CphExportResult
{
    public required string SourceDuckDbKey { get; init; }
    public required string SqliteKey { get; init; }
    public required int RowCount { get; init; }
    public required DateTime ExportedAt { get; init; }
}
