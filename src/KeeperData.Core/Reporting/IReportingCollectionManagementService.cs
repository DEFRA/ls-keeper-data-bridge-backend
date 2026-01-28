using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reporting;

/// <summary>
/// Service for managing MongoDB reporting and lineage collections.
/// These collections store import metadata, file processing details, and record lineage.
/// </summary>
public interface IReportingCollectionManagementService
{
    /// <summary>
    /// Deletes a specific reporting/lineage MongoDB collection by name.
    /// </summary>
    /// <param name="collectionName">The name of the reporting collection to delete</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result containing success status and collection name</returns>
    Task<DeleteReportingCollectionResult> DeleteReportingCollectionAsync(string collectionName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all reporting/lineage MongoDB collections.
    /// This includes: import_reports, import_files, record_lineage, record_lineage_events.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result containing list of deleted collections</returns>
    Task<DeleteAllReportingCollectionsResult> DeleteAllReportingCollectionsAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of deleting a single reporting collection.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Simple result record - no logic to test.")]
public record DeleteReportingCollectionResult
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
/// Result of deleting all reporting collections.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Simple result record - no logic to test.")]
public record DeleteAllReportingCollectionsResult
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