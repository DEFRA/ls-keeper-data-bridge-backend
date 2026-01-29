using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using CsvHelper;
using KeeperData.Bridge.Worker.Tasks;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Storage;
using KeeperData.Core.Telemetry;
using KeeperData.Infrastructure.Storage;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/[controller]")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class ImportController(
    ITaskProcessBulkFiles taskProcessBulkFiles,
    IImportReportingService importReportingService,
    ILogger<ImportController> logger,
    ICollectionManagementService collectionManagementService,
    IReportingCollectionManagementService reportingCollectionManagementService,
    IBlobStorageServiceFactory blobStorageServiceFactory,
    IApplicationMetrics metrics) : ControllerBase
{
    private readonly RecordIdGenerator _recordIdGenerator = new();

    /// <summary>
    /// Starts a bulk file import process asynchronously.
    /// Returns immediately with an import ID once the lock has been acquired.
    /// The import continues running in the background.
    /// </summary>
    /// <param name="sourceType">The source type for the import ("internal" or "external")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Import ID if started successfully, or an appropriate error response</returns>
    [HttpPost("start")]
    [ProducesResponseType(typeof(StartBulkImportResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status409Conflict)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> StartBulkImport([FromQuery] string sourceType = BlobStorageSources.External, CancellationToken cancellationToken = default)
    {
        var requestStopwatch = System.Diagnostics.Stopwatch.StartNew();

        logger.LogInformation("Received request to start bulk import with sourceType={sourceType} at {requestTime}", sourceType, DateTime.UtcNow);

        metrics.RecordRequest(MetricNames.Api, MetricNames.Operations.ApiRequests);
        metrics.RecordCount(MetricNames.Api, 1,
            (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiRequests),
            (MetricNames.CommonTags.Endpoint, "start"),
            (MetricNames.CommonTags.SourceType, sourceType));

        // Validate sourceType
        if (sourceType != BlobStorageSources.Internal && sourceType != BlobStorageSources.External)
        {
            logger.LogWarning("Invalid sourceType provided: {sourceType}", sourceType);

            requestStopwatch.Stop();
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.ErrorType, "validation"));
            metrics.RecordDuration("import_api_request", requestStopwatch.ElapsedMilliseconds);
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.Endpoint, "start"),
                (MetricNames.CommonTags.ErrorType, "validation"));

            return BadRequest(new ErrorResponse
            {
                Message = $"Invalid sourceType. Must be '{BlobStorageSources.Internal}' or '{BlobStorageSources.External}'.",
                Timestamp = DateTime.UtcNow
            });
        }

        try
        {
            var importId = await taskProcessBulkFiles.StartAsync(sourceType, cancellationToken);

            if (importId == null)
            {
                logger.LogWarning("Failed to start bulk import - could not acquire lock");

                requestStopwatch.Stop();
                metrics.RecordCount(MetricNames.Api, 1,
                    (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiConflicts),
                    ("conflict_type", "lock"));
                metrics.RecordDuration("import_api_request", requestStopwatch.ElapsedMilliseconds);
                metrics.RecordCount(MetricNames.Api, 1,
                    (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiConflicts),
                    (MetricNames.CommonTags.Endpoint, "start"),
                    ("conflict_type", "lock"));

                return Conflict(new ErrorResponse
                {
                    Message = "Import is already running. Please wait for the current import to complete.",
                    Timestamp = DateTime.UtcNow
                });
            }

            logger.LogInformation("Bulk import started successfully with importId={importId}, sourceType={sourceType}", importId.Value, sourceType);

            requestStopwatch.Stop();

            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiSuccesses),
                (MetricNames.CommonTags.SourceType, sourceType));
            metrics.RecordDuration("import_api_request", requestStopwatch.ElapsedMilliseconds);
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiSuccesses),
                (MetricNames.CommonTags.Endpoint, "start"),
                (MetricNames.CommonTags.SourceType, sourceType));

            return Accepted(new StartBulkImportResponse
            {
                ImportId = importId.Value,
                SourceType = sourceType,
                Message = "Import started successfully and is running in the background.",
                StartedAt = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Bulk import start request was cancelled");

            requestStopwatch.Stop();
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.ErrorType, "cancellation"));
            metrics.RecordDuration("import_api_request", requestStopwatch.ElapsedMilliseconds);
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.Endpoint, "start"),
                (MetricNames.CommonTags.ErrorType, "cancellation"));

            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error in StartBulkImport");

            requestStopwatch.Stop();
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.ErrorType, ex.GetType().Name));
            metrics.RecordDuration("import_api_request", requestStopwatch.ElapsedMilliseconds);
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.Endpoint, "start"),
                (MetricNames.CommonTags.ErrorType, ex.GetType().Name));

            throw;
        }
    }

    /// <summary>
    /// Gets a paginated list of import summaries in reverse chronological order (most recent first).
    /// Returns summary information including status, file counts, and record statistics for each import.
    /// </summary>
    /// <param name="skip">Number of records to skip for pagination (default: 0)</param>
    /// <param name="top">Number of records to return (default: 10, max: 100)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Paginated list of import summaries</returns>
    [HttpGet]
    [ProducesResponseType(typeof(ImportSummariesResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetImportSummaries([FromQuery] int skip = 0, [FromQuery] int top = 10, CancellationToken cancellationToken = default)
    {
        var requestStopwatch = Stopwatch.StartNew();
        logger.LogInformation("Received request to get import summaries with skip={skip}, top={top}", skip, top);

        // Validate parameters
        if (skip < 0)
        {
            logger.LogWarning("Invalid skip parameter: {skip}", skip);

            requestStopwatch.Stop();
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.Endpoint, "list"),
                (MetricNames.CommonTags.ErrorType, "general"));
            metrics.RecordDuration("import_api_request", requestStopwatch.ElapsedMilliseconds);
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.Endpoint, "list"),
                (MetricNames.CommonTags.ErrorType, "validation"),
                ("validation_field", "skip"));

            return BadRequest(new
            {
                Message = "Skip parameter must be greater than or equal to 0.",
                Timestamp = DateTime.UtcNow
            });
        }

        if (top <= 0 || top > 100)
        {
            logger.LogWarning("Invalid top parameter: {top}", top);

            requestStopwatch.Stop();
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.Endpoint, "list"),
                (MetricNames.CommonTags.ErrorType, "general"));
            metrics.RecordDuration("import_api_request", requestStopwatch.ElapsedMilliseconds);
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.Endpoint, "list"),
                (MetricNames.CommonTags.ErrorType, "validation"),
                ("validation_field", "top"));

            return BadRequest(new
            {
                Message = "Top parameter must be between 1 and 100.",
                Timestamp = DateTime.UtcNow
            });
        }

        try
        {
            var summaries = await importReportingService.GetImportSummariesAsync(skip, top, cancellationToken);

            logger.LogInformation("Successfully retrieved {count} import summaries", summaries.Count);

            requestStopwatch.Stop();
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiSuccesses),
                (MetricNames.CommonTags.Endpoint, "list"));
            metrics.RecordDuration("import_api_request", requestStopwatch.ElapsedMilliseconds);
            metrics.RecordCount(MetricNames.Api, summaries.Count,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiSummaries));

            return Ok(new ImportSummariesResponse
            {
                Skip = skip,
                Top = top,
                Count = summaries.Count,
                Imports = summaries,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get import summaries request was cancelled");

            requestStopwatch.Stop();
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.Endpoint, "list"),
                (MetricNames.CommonTags.ErrorType, "cancellation"));
            metrics.RecordDuration("import_api_request", requestStopwatch.ElapsedMilliseconds);
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.Endpoint, "list"),
                (MetricNames.CommonTags.ErrorType, "cancellation"));

            return StatusCode(499, new
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error in GetImportSummaries");

            requestStopwatch.Stop();
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.Endpoint, "list"),
                (MetricNames.CommonTags.ErrorType, ex.GetType().Name));
            metrics.RecordDuration("import_api_request", requestStopwatch.ElapsedMilliseconds);
            metrics.RecordCount(MetricNames.Api, 1,
                (MetricNames.CommonTags.Operation, MetricNames.Operations.ApiErrors),
                (MetricNames.CommonTags.Endpoint, "list"),
                ("error_type", ex.GetType().Name));

            throw;
        }
    }

    /// <summary>
    /// Gets the import report for a specific import ID.
    /// Includes overall import status, acquisition phase details, and ingestion phase details.
    /// </summary>
    /// <param name="importId">The import ID to retrieve the report for</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Import report if found, or 404 if not found</returns>
    [HttpGet("{importId}")]
    [ProducesResponseType(typeof(ImportReport), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetImportReport(Guid importId, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get import report for importId={importId}", importId);

        try
        {
            var report = await importReportingService.GetImportReportAsync(importId, cancellationToken);

            if (report == null)
            {
                logger.LogWarning("Import report not found for importId={importId}", importId);
                return NotFound(new
                {
                    Message = $"Import report not found for ImportId: {importId}",
                    ImportId = importId,
                    Timestamp = DateTime.UtcNow
                });
            }

            logger.LogInformation("Successfully retrieved import report for importId={importId}, status={status}",
                importId, report.Status);

            return Ok(report);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get import report request was cancelled for importId={importId}", importId);
            return StatusCode(499, new
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Gets the file processing reports for all files in a specific import.
    /// Includes details about acquisition and ingestion for each file.
    /// </summary>
    /// <param name="importId">The import ID to retrieve the file reports for</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>List of file processing reports</returns>
    [HttpGet("{importId}/files")]
    [ProducesResponseType(typeof(FileReportsResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetFileReports(Guid importId, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get file reports for importId={importId}", importId);

        try
        {
            // First check if the import exists
            var importReport = await importReportingService.GetImportReportAsync(importId, cancellationToken);

            if (importReport == null)
            {
                logger.LogWarning("Import not found for importId={importId}", importId);
                return NotFound(new
                {
                    Message = $"Import not found for ImportId: {importId}",
                    ImportId = importId,
                    Timestamp = DateTime.UtcNow
                });
            }

            var fileReports = await importReportingService.GetFileReportsAsync(importId, cancellationToken);

            logger.LogInformation("Successfully retrieved {fileCount} file report(s) for importId={importId}",
                fileReports.Count, importId);

            return Ok(new FileReportsResponse
            {
                ImportId = importId,
                TotalFiles = fileReports.Count,
                Files = fileReports,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get file reports request was cancelled for importId={importId}", importId);
            return StatusCode(499, new
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Gets the paginated lineage events for a specific record in chronological order.
    /// Shows the complete history of changes to a record across all imports.
    /// The recordId is a SHA256 hash generated by RecordIdGenerator from the composite key parts, making it URL-safe.
    /// </summary>
    /// <param name="collectionName">The collection name (e.g., "sam_cph_holdings")</param>
    /// <param name="recordId">The URL-safe record ID (SHA256 hash generated from primary key parts)</param>
    /// <param name="skip">Number of events to skip for pagination (default: 0)</param>
    /// <param name="top">Number of events to return (default: 10, max: 100)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Paginated list of lineage events</returns>
    [HttpGet("lineage/{collectionName}/{recordId}")]
    [ProducesResponseType(typeof(PaginatedLineageEvents), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> GetRecordLineageEvents(
        string collectionName,
        string recordId,
        [FromQuery] int skip = 0,
        [FromQuery] int top = 10,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get lineage events for {CollectionName}/{RecordId} with skip={Skip}, top={Top}",
            collectionName, recordId, skip, top);

        // Validate parameters
        if (skip < 0)
        {
            logger.LogWarning("Invalid skip parameter: {Skip}", skip);
            return BadRequest(new ErrorResponse
            {
                Message = "Skip parameter must be greater than or equal to 0.",
                Timestamp = DateTime.UtcNow
            });
        }

        if (top <= 0 || top > 100)
        {
            logger.LogWarning("Invalid top parameter: {Top}", top);
            return BadRequest(new ErrorResponse
            {
                Message = "Top parameter must be between 1 and 100.",
                Timestamp = DateTime.UtcNow
            });
        }

        // The recordId is a SHA256 hash generated by RecordIdGenerator from the composite key parts.
        // ASP.NET Core URL-decodes the route parameter, but since our hash is already URL-safe,
        // we can pass it directly to the reporting service.

        try
        {
            var result = await importReportingService.GetRecordLineageEventsPaginatedAsync(
                collectionName,
                recordId,
                skip,
                top,
                cancellationToken);

            logger.LogInformation("Successfully retrieved {Count} of {Total} lineage events for {CollectionName}/{RecordId}",
                result.Count, result.TotalEvents, collectionName, recordId);

            return Ok(result);
        }
        catch (KeyNotFoundException ex)
        {
            logger.LogWarning("Lineage not found for {CollectionName}/{RecordId}: {Message}",
                collectionName, recordId, ex.Message);
            return NotFound(new ErrorResponse
            {
                Message = ex.Message,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get lineage events request was cancelled for {CollectionName}/{RecordId}",
                collectionName, recordId);
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Deletes a specific MongoDB collection by name.
    /// The collection name must be defined in DataSetDefinitions.
    /// </summary>
    /// <param name="collectionName">The name of the collection to delete</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Success response with deleted collection name, or 404 if collection not found in definitions</returns>
    [HttpDelete("collections/{collectionName}")]
    [ProducesResponseType(typeof(DeleteCollectionResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> DeleteCollection(string collectionName, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to delete collection: {CollectionName}", collectionName);

        var result = await collectionManagementService.DeleteCollectionAsync(collectionName, cancellationToken);

        if (!result.Success)
        {
            if (result.Error is ArgumentException)
            {
                return NotFound(new ErrorResponse
                {
                    Message = result.Message,
                    Timestamp = DateTime.UtcNow
                });
            }

            if (result.Error is OperationCanceledException)
            {
                return StatusCode(499, new ErrorResponse
                {
                    Message = result.Message,
                    Timestamp = DateTime.UtcNow
                });
            }

            return StatusCode(500, new ErrorResponse
            {
                Message = result.Message,
                Timestamp = DateTime.UtcNow
            });
        }

        return Ok(new DeleteCollectionResponse
        {
            CollectionName = result.CollectionName,
            Success = true,
            Message = result.Message,
            DeletedAtUtc = result.OperatedAtUtc
        });
    }

    /// <summary>
    /// Deletes all MongoDB collections defined in DataSetDefinitions.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Summary of deleted collections</returns>
    [HttpDelete("collections")]
    [ProducesResponseType(typeof(DeleteCollectionsResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> DeleteAllCollections(CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to delete all collections");

        var result = await collectionManagementService.DeleteAllCollectionsAsync(cancellationToken);

        if (!result.Success)
        {
            if (result.Error is OperationCanceledException)
            {
                return StatusCode(499, new ErrorResponse
                {
                    Message = result.Message,
                    Timestamp = DateTime.UtcNow
                });
            }

            return StatusCode(500, new ErrorResponse
            {
                Message = result.Message,
                Timestamp = DateTime.UtcNow
            });
        }

        return Ok(new DeleteCollectionsResponse
        {
            DeletedCollections = result.DeletedCollections,
            TotalCount = result.TotalCount,
            Success = true,
            Message = result.Message,
            DeletedAtUtc = result.OperatedAtUtc
        });
    }

    /// <summary>
    /// Deletes a specific reporting/lineage MongoDB collection by name.
    /// Valid collections: import_reports, import_files, record_lineage, record_lineage_events.
    /// </summary>
    /// <param name="collectionName">The name of the reporting collection to delete</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Success response with deleted collection name, or 404 if collection not valid</returns>
    [HttpDelete("reporting-collections/{collectionName}")]
    [ProducesResponseType(typeof(DeleteCollectionResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> DeleteReportingCollection(string collectionName, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to delete reporting collection: {CollectionName}", collectionName);

        var result = await reportingCollectionManagementService.DeleteReportingCollectionAsync(collectionName, cancellationToken);

        if (!result.Success)
        {
            if (result.Error is ArgumentException)
            {
                return NotFound(new ErrorResponse
                {
                    Message = result.Message,
                    Timestamp = DateTime.UtcNow
                });
            }

            if (result.Error is OperationCanceledException)
            {
                return StatusCode(499, new ErrorResponse
                {
                    Message = result.Message,
                    Timestamp = DateTime.UtcNow
                });
            }

            return StatusCode(500, new ErrorResponse
            {
                Message = result.Message,
                Timestamp = DateTime.UtcNow
            });
        }

        return Ok(new DeleteCollectionResponse
        {
            CollectionName = result.CollectionName,
            Success = true,
            Message = result.Message,
            DeletedAtUtc = result.OperatedAtUtc
        });
    }

    /// <summary>
    /// Deletes all reporting/lineage MongoDB collections.
    /// This includes: import_reports, import_files, record_lineage, record_lineage_events.
    /// These collections will be automatically recreated when the next import runs.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Summary of deleted reporting collections</returns>
    [HttpDelete("reporting-collections")]
    [ProducesResponseType(typeof(DeleteCollectionsResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> DeleteAllReportingCollections(CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to delete all reporting collections");

        var result = await reportingCollectionManagementService.DeleteAllReportingCollectionsAsync(cancellationToken);

        if (!result.Success)
        {
            if (result.Error is OperationCanceledException)
            {
                return StatusCode(499, new ErrorResponse
                {
                    Message = result.Message,
                    Timestamp = DateTime.UtcNow
                });
            }

            return StatusCode(500, new ErrorResponse
            {
                Message = result.Message,
                Timestamp = DateTime.UtcNow
            });
        }

        return Ok(new DeleteCollectionsResponse
        {
            DeletedCollections = result.DeletedCollections,
            TotalCount = result.TotalCount,
            Success = true,
            Message = result.Message,
            DeletedAtUtc = result.OperatedAtUtc
        });
    }

    /// <summary>
    /// Generates a URL-safe record ID from composite key parts using SHA256 hashing.
    /// This endpoint helps clients construct the recordId needed for lineage queries.
    /// </summary>
    /// <param name="request">Request containing the key parts to hash</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Generated record ID hash</returns>
    [HttpPost("generate-record-id")]
    [ProducesResponseType(typeof(GenerateRecordIdResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    public IActionResult GenerateRecordId([FromBody] GenerateRecordIdRequest request, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to generate record ID from {count} key parts", request.KeyParts?.Length ?? 0);

        // Validate request
        if (request.KeyParts == null || request.KeyParts.Length == 0)
        {
            logger.LogWarning("Invalid request: KeyParts is null or empty");
            return BadRequest(new ErrorResponse
            {
                Message = "KeyParts must contain at least one value.",
                Timestamp = DateTime.UtcNow
            });
        }

        // Check for null or empty values in key parts
        for (int i = 0; i < request.KeyParts.Length; i++)
        {
            if (string.IsNullOrEmpty(request.KeyParts[i]))
            {
                logger.LogWarning("Invalid request: KeyPart at index {index} is null or empty", i);
                return BadRequest(new ErrorResponse
                {
                    Message = $"KeyPart at index {i} cannot be null or empty.",
                    Timestamp = DateTime.UtcNow
                });
            }
        }

        try
        {
            var recordId = _recordIdGenerator.GenerateId(request.KeyParts);

            logger.LogInformation("Successfully generated record ID: {recordId}", recordId);

            return Ok(new GenerateRecordIdResponse
            {
                RecordId = recordId,
                KeyParts = request.KeyParts,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to generate record ID");
            return BadRequest(new ErrorResponse
            {
                Message = $"Failed to generate record ID: {ex.Message}",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Deletes all objects from the internal target storage under the configured top-level folder prefix.
    /// This will clear down all files that were ingested into the target internal storage.
    /// </summary>
    /// <param name="sourceType">The source type for the storage (default is "internal")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Summary of deleted objects</returns>
    [HttpDelete("internal-storage")]
    [ProducesResponseType(typeof(ClearDownStorageResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> ClearDownInternalStorage([FromQuery] string sourceType = BlobStorageSources.Internal, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to clear down internal target storage");

        try
        {
            var blobStorageService = sourceType == BlobStorageSources.External
                ? blobStorageServiceFactory.GetSourceInternal() // the _fake_ external data store  (used for QA)
                : blobStorageServiceFactory.Get(); // the real internal data store

            var result = await blobStorageService.ClearDownAsync(cancellationToken);

            logger.LogInformation("Successfully cleared down internal storage. Total objects deleted: {TotalDeleted}",
                result.TotalDeleted);

            return Ok(new ClearDownStorageResponse
            {
                DeletedKeys = result.DeletedKeys,
                TotalDeleted = result.TotalDeleted,
                Success = true,
                Message = $"Successfully deleted {result.TotalDeleted} object(s) from internal target storage.",
                DeletedAtUtc = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Clear down internal storage request was cancelled");
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to clear down internal storage");
            throw;
        }
    }
}

/// <summary>
/// Response from clearing down internal storage.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record ClearDownStorageResponse
{
    /// <summary>
    /// List of object keys that were deleted.
    /// </summary>
    public required IReadOnlyList<string> DeletedKeys { get; init; }

    /// <summary>
    /// Total number of objects deleted.
    /// </summary>
    public required int TotalDeleted { get; init; }

    /// <summary>
    /// Indicates whether the operation was successful.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Success message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// UTC timestamp when the clear down operation completed.
    /// </summary>
    public DateTime DeletedAtUtc { get; init; }
}

/// <summary>
/// Request to generate a record ID from composite key parts.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record GenerateRecordIdRequest
{
    /// <summary>
    /// The individual parts of the composite key (in order).
    /// Each part will be joined and hashed to create the record ID.
    /// </summary>
    /// <example>["NORTH", "F001"]</example>
    public required string[] KeyParts { get; init; }
}

/// <summary>
/// Response containing the generated record ID.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record GenerateRecordIdResponse
{
    /// <summary>
    /// The generated URL-safe record ID (SHA256 hash, 43 characters).
    /// This ID can be used to query lineage events.
    /// </summary>
    public required string RecordId { get; init; }

    /// <summary>
    /// The key parts that were used to generate the record ID.
    /// </summary>
    public required string[] KeyParts { get; init; }

    /// <summary>
    /// The timestamp when the ID was generated.
    /// </summary>
    public DateTime Timestamp { get; init; }
}