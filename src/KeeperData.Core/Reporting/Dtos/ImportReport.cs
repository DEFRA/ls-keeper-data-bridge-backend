namespace KeeperData.Core.Reporting.Dtos;

public record ImportReport
{
    public required Guid ImportId { get; init; }
    public required string SourceType { get; init; }
    public required ImportStatus Status { get; init; }
    public DateTime StartedAtUtc { get; init; }
    public DateTime? CompletedAtUtc { get; init; }
    public AcquisitionPhaseReport? AcquisitionPhase { get; init; }
    public IngestionPhaseReport? IngestionPhase { get; init; }
    public string? Error { get; init; }
}

public record AcquisitionPhaseReport
{
    public required PhaseStatus Status { get; init; }
    public int FilesDiscovered { get; init; }
    public int FilesProcessed { get; init; }
    public int FilesFailed { get; init; }
    public DateTime? StartedAtUtc { get; init; }
    public DateTime? CompletedAtUtc { get; init; }
}

public record IngestionPhaseReport
{
    public required PhaseStatus Status { get; init; }
    public int FilesProcessed { get; init; }
    public int RecordsCreated { get; init; }
    public int RecordsUpdated { get; init; }
    public int RecordsDeleted { get; init; }
    public DateTime? StartedAtUtc { get; init; }
    public DateTime? CompletedAtUtc { get; init; }
}