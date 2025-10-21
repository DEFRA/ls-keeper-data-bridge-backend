namespace KeeperData.Core.Reporting.Dtos;

public record AcquisitionPhaseUpdate
{
    public PhaseStatus Status { get; init; }
    public int FilesDiscovered { get; init; }
    public int FilesProcessed { get; init; }
    public int FilesFailed { get; init; }
    public DateTime? CompletedAtUtc { get; init; }
}