using KeeperData.Core.Querying.Models;

namespace KeeperData.Core.Querying.Models;

/// <summary>
/// Parameters for querying collections using abstracted .NET types (no database-specific types).
/// </summary>
public class QueryParameters
{
    /// <summary>
    /// Name of the collection to query
    /// </summary>
    public required string CollectionName { get; init; }

    /// <summary>
    /// Filter expression (null or EmptyFilter matches all documents)
    /// </summary>
    public FilterExpression? Filter { get; init; }

    /// <summary>
    /// Sort expression (null means no specific ordering)
    /// </summary>
    public SortExpression? Sort { get; init; }

    /// <summary>
    /// Fields to include in the result. Null means all fields.
    /// </summary>
    public IReadOnlyList<string>? FieldsToSelect { get; init; }

    /// <summary>
    /// Number of records to skip for pagination
    /// </summary>
    public int Skip { get; init; }

    /// <summary>
    /// Number of records to return (page size)
    /// </summary>
    public int Top { get; init; }

    /// <summary>
    /// Whether to include total count in the response
    /// </summary>
    public bool IncludeCount { get; init; } = true;
}
