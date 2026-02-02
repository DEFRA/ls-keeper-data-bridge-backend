using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reporting.Dtos;

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record AcquisitionPhaseUpdate
{
    public PhaseStatus Status { get; init; }
    public int FilesDiscovered { get; init; }
    public int FilesProcessed { get; init; }
    public int FilesFailed { get; init; }
    public DateTime? CompletedAtUtc { get; init; }
}