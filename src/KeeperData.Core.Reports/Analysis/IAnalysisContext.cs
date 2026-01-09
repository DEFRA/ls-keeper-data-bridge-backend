using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Models;

namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Provides scoped context for an analysis session, including a query cache.
/// All data access during analysis should go through this context.
/// </summary>
public interface IAnalysisContext
{
    /// <summary>
    /// Gets the operation identifier for this analysis session.
    /// </summary>
    string OperationId { get; }

    /// <summary>
    /// Gets the data set definitions.
    /// </summary>
    DataSetDefinitions DataSets { get; }

    /// <summary>
    /// Queries data with automatic caching. Cache key is derived from QueryParameters.
    /// </summary>
    /// <param name="parameters">The query parameters.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The query result, from cache if available.</returns>
    Task<QueryResult> QueryAsync(QueryParameters parameters, CancellationToken ct);

    /// <summary>
    /// Queries for a single record with automatic caching.
    /// </summary>
    /// <param name="parameters">The query parameters (Top should be 1).</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The first record, or null if not found.</returns>
    Task<Dictionary<string, object?>?> QuerySingleAsync(QueryParameters parameters, CancellationToken ct);
}
