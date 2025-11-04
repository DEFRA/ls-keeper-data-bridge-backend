namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents the response when a specific MongoDB collection is successfully deleted.
/// </summary>
public record DeleteCollectionResponse
{
    /// <summary>
    /// Gets the name of the deleted collection.
    /// </summary>
    public required string CollectionName { get; init; }

    /// <summary>
    /// Gets a value indicating whether the deletion was successful.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Gets the success message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the collection was deleted.
    /// </summary>
    public DateTime DeletedAtUtc { get; init; }
}
