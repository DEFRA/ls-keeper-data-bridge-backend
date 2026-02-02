using KeeperData.Core.Querying.Models;

namespace KeeperData.Core.Querying.Abstract;

/// <summary>
/// Service for querying collections using OData-style parameters.
/// This is a facade over IQueryService that handles OData-to-.NET conversion.
/// </summary>
public interface IODataQueryService
{
    /// <summary>
    /// Queries a collection using OData query parameters.
    /// </summary>
    /// <param name="collectionName">Name of the collection to query (must be defined in DataSetDefinitions)</param>
    /// <param name="filter">OData $filter expression (e.g., "CPH eq 'ABC123' and IsDeleted eq false")</param>
    /// <param name="orderBy">OData $orderby expression (e.g., "UpdatedAtUtc desc")</param>
    /// <param name="select">OData $select expression (e.g., "CPH,UpdatedAtUtc,IsDeleted")</param>
    /// <param name="skip">Number of records to skip for pagination</param>
    /// <param name="top">Number of records to take (max page size)</param>
    /// <param name="count">Whether to include total count in the response</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Query result containing data and pagination metadata</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("SonarQube", "S107", Justification = "OData query parameters are standard and well-understood")]
    Task<QueryResult> QueryAsync(
        string collectionName,
        string? filter = null,
        string? orderBy = null,
        string? select = null,
        int? skip = null,
        int? top = null,
        bool count = true,
        CancellationToken cancellationToken = default);
}
