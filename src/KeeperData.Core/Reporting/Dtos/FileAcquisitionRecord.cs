namespace KeeperData.Core.Reporting.Dtos;

public record FileAcquisitionRecord
{
    public required string FileName { get; init; }
    public required string FileKey { get; init; }
    public required string DatasetName { get; init; }
    public required string Md5Hash { get; init; }
    public long FileSize { get; init; }
    public required string SourceKey { get; init; }
    public long DecryptionDurationMs { get; init; }
    public DateTime AcquiredAtUtc { get; init; }
    public FileProcessingStatus Status { get; init; }
    public string? Error { get; init; }
}