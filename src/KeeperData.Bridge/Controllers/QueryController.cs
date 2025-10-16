using KeeperData.Core.Querying.Abstract;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

/// <summary>
/// Controller for querying MongoDB collections dynamically with OData-style filters.
/// </summary>
[ApiController]
[Route("api/query")]
public class QueryController : ControllerBase
{
    private readonly IMongoQueryService _queryService;
    private readonly ILogger<QueryController> _logger;

    public QueryController(
        IMongoQueryService queryService,
        ILogger<QueryController> logger)
    {
        _queryService = queryService ?? throw new ArgumentNullException(nameof(queryService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Query a MongoDB collection with OData-style parameters.
    /// </summary>
    /// <param name="collectionName">Name of the collection to query (e.g., "sam_cph_holdings")</param>
    /// <param name="filter">OData $filter expression (e.g., "CPH eq 'ABC123' and IsDeleted eq false")</param>
    /// <param name="orderby">OData $orderby expression (e.g., "UpdatedAtUtc desc" or "CPH asc, UpdatedAtUtc desc")</param>
    /// <param name="skip">Number of records to skip for pagination (OData $skip)</param>
    /// <param name="top">Number of records to return (OData $top, max 1000, default 100)</param>
    /// <param name="count">Whether to include total count in response (OData $count, default true)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Query results with data and pagination metadata</returns>
    /// <response code="200">Query executed successfully</response>
    /// <response code="400">Invalid query parameters</response>
    /// <response code="404">Collection not found</response>
    /// <response code="500">Internal server error</response>
    /// <example>
    /// GET /api/query/sam_cph_holdings?$filter=CPH eq 'ABC123'&amp;$top=50
    /// GET /api/query/sam_cph_holdings?$filter=contains(CPH,'ABC') and IsDeleted eq false&amp;$orderby=UpdatedAtUtc desc&amp;$skip=20&amp;$top=10
    /// </example>
    [HttpGet("{collectionName}")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> Query(
        [FromRoute] string collectionName,
        [FromQuery(Name = "$filter")] string? filter = null,
        [FromQuery(Name = "$orderby")] string? orderby = null,
        [FromQuery(Name = "$skip")] int? skip = null,
        [FromQuery(Name = "$top")] int? top = null,
        [FromQuery(Name = "$count")] bool count = true,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Received query request for collection '{CollectionName}' with filter='{Filter}', orderby='{OrderBy}', skip={Skip}, top={Top}",
            collectionName, filter ?? "none", orderby ?? "none", skip, top);

        try
        {
            var result = await _queryService.QueryAsync(
                collectionName,
                filter,
                orderby,
                skip,
                top,
                count,
                cancellationToken);

            _logger.LogInformation(
                "Query completed successfully for collection '{CollectionName}'. Returned {Count} records",
                collectionName, result.Count);

            return Ok(result);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(ex, "Invalid query parameters for collection '{CollectionName}'", collectionName);
            return BadRequest(new
            {
                Message = ex.Message,
                Parameter = ex.ParamName,
                CollectionName = collectionName,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Query request cancelled for collection '{CollectionName}'", collectionName);
            return StatusCode(499, new
            {
                Message = "Request was cancelled",
                CollectionName = collectionName,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing query on collection '{CollectionName}'", collectionName);
            return StatusCode(500, new
            {
                Message = "An error occurred while executing the query",
                Error = ex.Message,
                CollectionName = collectionName,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Get list of available collections that can be queried.
    /// </summary>
    /// <returns>List of queryable collection names</returns>
    /// <response code="200">List of collections retrieved successfully</response>
    [HttpGet]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    public IActionResult GetCollections()
    {
        _logger.LogInformation("Received request for available collections");

        try
        {
            // This would need to be implemented by injecting IDataSetDefinitions
            // For now, return a simple message
            return Ok(new
            {
                Message = "Use GET /api/query/{collectionName} to query a specific collection",
                Examples = new[]
                {
                    "/api/query/sam_cph_holdings?$filter=CPH eq 'ABC123'",
                    "/api/query/sam_cph_holdings?$filter=IsDeleted eq false&$orderby=UpdatedAtUtc desc&$top=50",
                    "/api/query/sam_cph_holdings?$filter=contains(CPH,'ABC')&$skip=20&$top=10"
                },
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving collections list");
            return StatusCode(500, new
            {
                Message = "An error occurred while retrieving the collections list",
                Error = ex.Message,
                Timestamp = DateTime.UtcNow
            });
        }
    }
}
