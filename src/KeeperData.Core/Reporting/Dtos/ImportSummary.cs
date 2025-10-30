namespace KeeperData.Core.Reporting.Dtos;

public record ImportSummary
{
    public required Guid ImportId { get; init; }
    public required ImportStatus Status { get; init; }
    public DateTime StartedAtUtc { get; init; }
    public DateTime? CompletedAtUtc { get; init; }
    public int FilesProcessed { get; init; }
    public int RecordsCreated { get; init; }
    public int RecordsUpdated { get; init; }
    public int RecordsDeleted { get; init; }
}
