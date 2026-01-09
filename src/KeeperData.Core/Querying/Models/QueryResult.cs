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
    /// OData select applied
    /// </summary>
    public string? Select { get; init; }

    /// <summary>
    /// Timestamp when the query was executed
    /// </summary>
    public DateTime ExecutedAtUtc { get; init; } = DateTime.UtcNow;

    /// <summary>
    /// Combines multiple QueryResult instances into a single QueryResult
    /// </summary>
    /// <param name="results">Array of QueryResult instances to combine</param>
    /// <returns>A single QueryResult containing all data from the input results</returns>
    public static QueryResult Combine(params QueryResult[] results)
    {
        if (results is null || results.Length == 0)
        {
            return new QueryResult { CollectionName = string.Empty, Count = 0, Data = [], ExecutedAtUtc = DateTime.UtcNow };
        }

        var f = results.First();

        var combinedData = results
            .Where(r => r?.Data is not null)
            .SelectMany(r => r.Data)
            .ToList();

        return new QueryResult
        {
            Data = combinedData,
            CollectionName = f.CollectionName,
            Count = combinedData.Count,
            TotalCount = results.Sum(x => x.TotalCount),
            ExecutedAtUtc = DateTime.UtcNow
        };
    }
}