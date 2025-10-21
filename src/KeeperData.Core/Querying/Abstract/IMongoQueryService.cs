using KeeperData.Core.Querying.Models;

namespace KeeperData.Core.Querying.Abstract;

/// <summary>
/// Service for querying MongoDB collections dynamically with OData support.
/// </summary>
public interface IMongoQueryService
{
    /// <summary>
    /// Queries a MongoDB collection using OData query parameters.
    /// </summary>
    /// <param name="collectionName">Name of the collection to query (must be defined in DataSetDefinitions)</param>
    /// <param name="filter">OData $filter expression (e.g., "CPH eq 'ABC123' and IsDeleted eq false")</param>
    /// <param name="orderBy">OData $orderby expression (e.g., "UpdatedAtUtc desc")</param>
    /// <param name="skip">Number of records to skip for pagination</param>
    /// <param name="top">Number of records to take (max page size)</param>
    /// <param name="count">Whether to include total count in the response</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Query result containing data and pagination metadata</returns>
    Task<QueryResult> QueryAsync(
        string collectionName,
        string? filter = null,
        string? orderBy = null,
        int? skip = null,
        int? top = null,
        bool count = true,
        CancellationToken cancellationToken = default);
}