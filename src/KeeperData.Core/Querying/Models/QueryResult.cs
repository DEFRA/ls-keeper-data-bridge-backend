namespace KeeperData.Core.Querying.Models;

/// <summary>
/// Result of a MongoDB query operation with pagination metadata.
/// </summary>
public class QueryResult
{
    /// <summary>
    /// The collection that was queried
    /// </summary>
    public required string CollectionName { get; init; }

    /// <summary>
    /// Array of documents as dictionaries (dynamic schema support)
    /// </summary>
    public required IReadOnlyList<Dictionary<string, object?>> Data { get; init; }

    /// <summary>
    /// Number of records in the current page
    /// </summary>
    public required int Count { get; init; }

    /// <summary>
    /// Total number of records matching the filter (if requested)
    /// </summary>
    public long? TotalCount { get; init; }

    /// <summary>
    /// Number of records skipped (pagination offset)
    /// </summary>
    public int? Skip { get; init; }

    /// <summary>
    /// Number of records requested (page size)
    /// </summary>
    public int? Top { get; init; }

    /// <summary>
    /// OData filter applied
    /// </summary>
    public string? Filter { get; init; }

    /// <summary>
    /// OData orderby applied
    /// </summary>
    public string? OrderBy { get; init; }

    /// <summary>
    /// Timestamp when the query was executed
    /// </summary>
    public DateTime ExecutedAtUtc { get; init; } = DateTime.UtcNow;
}