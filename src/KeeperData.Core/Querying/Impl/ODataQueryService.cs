using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Models;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Querying.Impl;

/// <summary>
/// OData adapter service that converts OData query parameters to custom filter/sort expressions
/// and delegates to QueryService for execution.
/// </summary>
public class ODataQueryService : IODataQueryService
{
    private readonly IQueryService _queryService;
    private readonly ILogger<ODataQueryService> _logger;

    public ODataQueryService(
        IQueryService queryService,
        ILogger<ODataQueryService> logger)
    {
        _queryService = queryService ?? throw new ArgumentNullException(nameof(queryService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<QueryResult> QueryAsync(
        string collectionName,
        string? filter = null,
        string? orderBy = null,
        string? select = null,
        int? skip = null,
        int? top = null,
        bool count = true,
        CancellationToken cancellationToken = default)
    {
        // Validate top parameter - must be greater than 0 if specified
        if (top.HasValue && top.Value <= 0)
        {
            throw new ArgumentException("Top parameter must be greater than 0", nameof(top));
        }

        _logger.LogInformation(
            "Converting OData query to custom expressions for collection '{CollectionName}' with filter='{Filter}', orderBy='{OrderBy}', select='{Select}', skip={Skip}, top={Top}",
            collectionName, filter ?? "none", orderBy ?? "none", select ?? "none", skip, top);

        var filterExpression = BuildFilterExpression(filter);
        var sortExpression = BuildSortExpression(orderBy);
        var fieldsToSelect = ParseSelectExpression(select);

        var parameters = new QueryParameters
        {
            CollectionName = collectionName,
            Filter = filterExpression,
            Sort = sortExpression,
            FieldsToSelect = fieldsToSelect,
            Skip = skip ?? 0,
            Top = top ?? 0,
            IncludeCount = count
        };

        var result = await _queryService.QueryAsync(parameters, cancellationToken);

        return new QueryResult
        {
            CollectionName = result.CollectionName,
            Data = result.Data,
            Count = result.Count,
            TotalCount = result.TotalCount,
            Skip = result.Skip,
            Top = result.Top,
            Filter = filter,
            OrderBy = orderBy,
            Select = select,
            ExecutedAtUtc = result.ExecutedAtUtc
        };
    }

    private FilterExpression? BuildFilterExpression(string? filter)
    {
        if (string.IsNullOrWhiteSpace(filter))
        {
            return null;
        }

        try
        {
            var translator = new ODataToFilterExpressionTranslator();
            return translator.Parse(filter);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to parse filter: {Filter}", filter);
            throw new ArgumentException($"Invalid filter expression: {ex.Message}", nameof(filter), ex);
        }
    }

    private SortExpression? BuildSortExpression(string? orderBy)
    {
        if (string.IsNullOrWhiteSpace(orderBy))
        {
            return null;
        }

        try
        {
            var parser = new ODataToSortExpressionParser();
            return parser.Parse(orderBy);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to parse orderBy: {OrderBy}", orderBy);
            throw new ArgumentException($"Invalid orderBy expression: {ex.Message}", nameof(orderBy), ex);
        }
    }

    private IReadOnlyList<string>? ParseSelectExpression(string? select)
    {
        if (string.IsNullOrWhiteSpace(select))
        {
            return null;
        }

        try
        {
            var parser = new ODataSelectParser();
            return parser.Parse(select);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to parse select: {Select}", select);
            throw new ArgumentException($"Invalid select expression: {ex.Message}", nameof(select), ex);
        }
    }
}
