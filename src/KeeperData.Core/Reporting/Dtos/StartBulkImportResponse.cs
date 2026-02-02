using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents the response when a bulk import is successfully started.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record StartBulkImportResponse
{
    /// <summary>
    /// Gets the unique identifier for the started import.
    /// </summary>
    public required Guid ImportId { get; init; }

    /// <summary>
    /// Gets the source type of the import ("internal" or "external").
    /// </summary>
    public required string SourceType { get; init; }

    /// <summary>
    /// Gets the success message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the import was started.
    /// </summary>
    public DateTime StartedAt { get; init; }
}