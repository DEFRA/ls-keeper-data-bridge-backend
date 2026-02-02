using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents the response when multiple MongoDB collections are successfully deleted.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record DeleteCollectionsResponse
{
    /// <summary>
    /// Gets the list of collection names that were deleted.
    /// </summary>
    public required IReadOnlyList<string> DeletedCollections { get; init; }

    /// <summary>
    /// Gets the total number of collections deleted.
    /// </summary>
    public required int TotalCount { get; init; }

    /// <summary>
    /// Gets a value indicating whether the deletion operation was successful.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Gets the success message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the collections were deleted.
    /// </summary>
    public DateTime DeletedAtUtc { get; init; }
}