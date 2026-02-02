using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents the response containing file processing reports for a specific import.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record FileReportsResponse
{
    /// <summary>
    /// Gets the import ID these file reports belong to.
    /// </summary>
    public required Guid ImportId { get; init; }

    /// <summary>
    /// Gets the total number of files in this import.
    /// </summary>
    public int TotalFiles { get; init; }

    /// <summary>
    /// Gets the list of file processing reports.
    /// </summary>
    public required IReadOnlyList<FileProcessingReport> Files { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the response.
    /// </summary>
    public DateTime Timestamp { get; init; }
}