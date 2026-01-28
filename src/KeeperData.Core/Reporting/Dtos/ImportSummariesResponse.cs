using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents the response for a paginated list of import summaries.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record ImportSummariesResponse
{
    /// <summary>
    /// Gets the number of records skipped in the pagination.
    /// </summary>
    public int Skip { get; init; }

    /// <summary>
    /// Gets the maximum number of records requested.
    /// </summary>
    public int Top { get; init; }

    /// <summary>
    /// Gets the actual number of import summaries returned.
    /// </summary>
    public int Count { get; init; }

    /// <summary>
    /// Gets the list of import summaries.
    /// </summary>
    public required IReadOnlyList<ImportSummary> Imports { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the response.
    /// </summary>
    public DateTime Timestamp { get; init; }
}