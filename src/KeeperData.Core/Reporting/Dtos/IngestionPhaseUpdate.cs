namespace KeeperData.Core.Reporting.Dtos;

public record IngestionPhaseUpdate
{
    public PhaseStatus Status { get; init; }
    public int FilesProcessed { get; init; }
    public int RecordsCreated { get; init; }
    public int RecordsUpdated { get; init; }
    public int RecordsDeleted { get; init; }
    public DateTime? CompletedAtUtc { get; init; }
}
