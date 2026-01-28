using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Models;

namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Provides a scoped analysis context with query caching for the duration of an analysis session.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Analysis context with query service dependency - covered by integration tests.")]
public sealed class AnalysisContext : IAnalysisContext
{
    private readonly IQueryService _queryService;
    private readonly ConcurrentDictionary<string, Lazy<Task<QueryResult>>> _cache = new();

    public AnalysisContext(string operationId, IQueryService queryService, DataSetDefinitions dataSets)
    {
        OperationId = operationId;
        _queryService = queryService;
        DataSets = dataSets;
    }

    /// <inheritdoc />
    public string OperationId { get; }

    /// <inheritdoc />
    public DataSetDefinitions DataSets { get; }

    /// <inheritdoc />
    public async Task<QueryResult> QueryAsync(QueryParameters parameters, CancellationToken ct)
    {
        var cacheKey = GenerateCacheKey(parameters);

        var lazyTask = _cache.GetOrAdd(cacheKey, _ => new Lazy<Task<QueryResult>>(
            () => _queryService.QueryAsync(parameters, ct),
            LazyThreadSafetyMode.ExecutionAndPublication));

        return await lazyTask.Value;
    }

    /// <inheritdoc />
    public async Task<Dictionary<string, object?>?> QuerySingleAsync(QueryParameters parameters, CancellationToken ct)
    {
        var result = await QueryAsync(parameters, ct);
        return result.Data.FirstOrDefault();
    }

    private static string GenerateCacheKey(QueryParameters parameters)
    {
        var sb = new StringBuilder();
        sb.Append(parameters.CollectionName);
        sb.Append('|');
        sb.Append(parameters.Filter?.ToString() ?? string.Empty);
        sb.Append('|');
        sb.Append(parameters.Skip);
        sb.Append('|');
        sb.Append(parameters.Top);
        sb.Append('|');
        sb.Append(parameters.Sort?.ToString() ?? string.Empty);
        sb.Append('|');
        sb.Append(parameters.IncludeCount);

        if (parameters.FieldsToSelect is { Count: > 0 })
        {
            sb.Append('|');
            sb.Append(string.Join(',', parameters.FieldsToSelect));
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString()));
        return Convert.ToHexString(hash);
    }
}
