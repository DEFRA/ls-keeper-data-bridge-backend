namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents a comprehensive report of an import operation, including acquisition and ingestion phases.
/// </summary>
public record ImportReport
{
    /// <summary>
    /// Gets the unique identifier for the import.
    /// </summary>
    public required Guid ImportId { get; set; }

    /// <summary>
    /// Gets the source type of the import (e.g., "internal" or "external").
    /// </summary>
    public required string SourceType { get; set; }

    /// <summary>
    /// Gets the overall status of the import (Started, Completed, or Failed).
    /// </summary>
    public required ImportStatus Status { get; set; }

    /// <summary>
    /// Gets the UTC timestamp when the import started.
    /// </summary>
    public DateTime StartedAtUtc { get; set; }

    /// <summary>
    /// Gets the UTC timestamp when the import completed, or null if still in progress.
    /// </summary>
    public DateTime? CompletedAtUtc { get; set; }

    /// <summary>
    /// Gets the report for the acquisition phase, or null if not yet started.
    /// </summary>
    public AcquisitionPhaseReport? AcquisitionPhase { get; set; }

    /// <summary>
    /// Gets the report for the ingestion phase, or null if not yet started.
    /// </summary>
    public IngestionPhaseReport? IngestionPhase { get; set; }

    /// <summary>
    /// Gets the error message if the import failed, or null if successful.
    /// </summary>
    public string? Error { get; set; }
}

/// <summary>
/// Represents details of the acquisition phase, where files are discovered and acquired.
/// </summary>
public record AcquisitionPhaseReport
{
    /// <summary>
    /// Gets the current status of the acquisition phase.
    /// </summary>
    public required PhaseStatus Status { get; set; }

    /// <summary>
    /// Gets the total number of files discovered.
    /// </summary>
    public int FilesDiscovered { get; set; }

    /// <summary>
    /// Gets the total number of files successfully processed.
    /// </summary>
    public int FilesProcessed { get; set; }

    /// <summary>
    /// Gets the total number of files that failed to process.
    /// </summary>
    public int FilesFailed { get; set; }

    /// <summary>
    /// Gets the total number of files that wereskipped.
    /// </summary>
    public int FilesSkipped { get; set; }

    /// <summary>
    /// Gets the UTC timestamp when the acquisition phase started, or null if not yet started.
    /// </summary>
    public DateTime? StartedAtUtc { get; set; }

    /// <summary>
    /// Gets the UTC timestamp when the acquisition phase completed, or null if still in progress.
    /// </summary>
    public DateTime? CompletedAtUtc { get; set; }
}

/// <summary>
/// Represents details of the ingestion phase, where acquired files are processed and data is ingested.
/// </summary>
public record IngestionPhaseReport
{
    /// <summary>
    /// Gets the current status of the ingestion phase.
    /// </summary>
    public required PhaseStatus Status { get; set; }

    /// <summary>
    /// Gets the total number of files processed during ingestion.
    /// </summary>
    public int FilesProcessed { get; set; }

    /// <summary>
    /// Gets the total number of new records created during ingestion.
    /// </summary>
    public int RecordsCreated { get; set; }

    /// <summary>
    /// Gets the total number of records updated during ingestion.
    /// </summary>
    public int RecordsUpdated { get; set; }

    /// <summary>
    /// Gets the total number of records deleted during ingestion.
    /// </summary>
    public int RecordsDeleted { get; set; }

    /// <summary>
    /// Gets the UTC timestamp when the ingestion phase started, or null if not yet started.
    /// </summary>
    public DateTime? StartedAtUtc { get; set; }

    /// <summary>
    /// Gets the UTC timestamp when the ingestion phase completed, or null if still in progress.
    /// </summary>
    public DateTime? CompletedAtUtc { get; set; }

    public IngestionCurrentFileStatus? CurrentFileStatus { get; set; }

    public int FilesSkipped { get; set; }
}

public record IngestionCurrentFileStatus
{
    /// <summary>
    /// The current file being processed.
    /// </summary>
    public string? FileName { get; set; }

    /// <summary>
    /// Total number of rows in the current file.
    /// </summary>
    public int? TotalRows { get; set; }

    /// <summary>
    /// Gets or sets index of the current row within a collection or result set.
    /// </summary>
    public int? RowNumber { get; set; }

    /// <summary>
    /// Gets or sets the percentage of the operation that has been completed.
    /// </summary>
    public int? PercentageCompleted { get; set; }

    public decimal? RowsPerMinute { get; set; }

    public TimeSpan? EstimatedTimeRemaining { get; set; }

    public DateTime? EstimatedCompletionUtc { get; set; }
}