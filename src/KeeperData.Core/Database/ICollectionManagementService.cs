using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Database;

/// <summary>
/// Service for managing MongoDB collections defined in DataSetDefinitions.
/// </summary>
public interface ICollectionManagementService
{
    /// <summary>
    /// Deletes a specific MongoDB collection by name.
    /// The collection name must be defined in DataSetDefinitions.
    /// </summary>
    /// <param name="collectionName">The name of the collection to delete</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result containing success status and collection name</returns>
    Task<DeleteCollectionResult> DeleteCollectionAsync(string collectionName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all MongoDB collections defined in DataSetDefinitions.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result containing list of deleted collections</returns>
    Task<DeleteAllCollectionsResult> DeleteAllCollectionsAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of deleting a single collection.
/// </summary>
public record DeleteCollectionResult
{
    /// <summary>
    /// Gets the name of the deleted collection.
    /// </summary>
    public required string CollectionName { get; init; }

    /// <summary>
    /// Gets a value indicating whether the operation was successful.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Gets the result message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the error exception if the operation failed, or null if successful.
    /// </summary>
    public Exception? Error { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the operation.
    /// </summary>
    public DateTime OperatedAtUtc { get; init; }
}

/// <summary>
/// Result of deleting all collections.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record DeleteAllCollectionsResult
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
    /// Gets a value indicating whether all collections were deleted successfully.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Gets the result message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the error exception if the operation failed, or null if successful.
    /// </summary>
    public Exception? Error { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the operation.
    /// </summary>
    public DateTime OperatedAtUtc { get; init; }
}