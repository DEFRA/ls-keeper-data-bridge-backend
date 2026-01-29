using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Dtos;

/// <summary>
/// Represents the result of a cleanse data deletion operation.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record CleanseDeleteResult
{
    /// <summary>
    /// Gets a value indicating whether the deletion was successful.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Gets the name of the deleted collection.
    /// </summary>
    public required string CollectionName { get; init; }

    /// <summary>
    /// Gets a message describing the result.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the number of documents deleted, if available.
    /// </summary>
    public long? DeletedCount { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the operation was performed.
    /// </summary>
    public DateTime OperatedAtUtc { get; init; }

    /// <summary>
    /// Gets the error that occurred, if any.
    /// </summary>
    public Exception? Error { get; init; }
}
