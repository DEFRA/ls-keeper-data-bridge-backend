using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents a summary of an import operation with key metrics.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record ImportSummary
{
    /// <summary>
    /// Gets the unique identifier for the import.
    /// </summary>
    public required Guid ImportId { get; init; }

    /// <summary>
    /// Gets the current status of the import (Started, Completed, or Failed).
    /// </summary>
    public required ImportStatus Status { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the import started.
    /// </summary>
    public DateTime StartedAtUtc { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the import completed, or null if still in progress.
    /// </summary>
    public DateTime? CompletedAtUtc { get; init; }

    /// <summary>
    /// Gets the total number of files processed during ingestion.
    /// </summary>
    public int FilesProcessed { get; init; }

    /// <summary>
    /// Gets the total number of records created during the import.
    /// </summary>
    public int RecordsCreated { get; init; }

    /// <summary>
    /// Gets the total number of records updated during the import.
    /// </summary>
    public int RecordsUpdated { get; init; }

    /// <summary>
    /// Gets the total number of records deleted during the import.
    /// </summary>
    public int RecordsDeleted { get; init; }
}