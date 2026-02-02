using KeeperData.Core.Querying.Models;

namespace KeeperData.Core.Querying.Abstract;

/// <summary>
/// Service for querying MongoDB collections using native .NET types.
/// This service has no dependency on OData and can be used in non-web contexts.
/// </summary>
public interface IQueryService
{
    /// <summary>
    /// Queries a MongoDB collection using native .NET query parameters.
    /// </summary>
    /// <param name="parameters">Query parameters including filter, sort, select, and pagination</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Query result containing data and pagination metadata</returns>
    Task<QueryResult> QueryAsync(
        QueryParameters parameters,
        CancellationToken cancellationToken = default);
}
