using KeeperData.Bridge.Worker.Tasks;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Infrastructure.Storage;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/[controller]")]
public class ImportController(
    ITaskProcessBulkFiles taskProcessBulkFiles,
    IImportReportingService importReportingService,
    ILogger<ImportController> logger,
    ICollectionManagementService collectionManagementService,
    IReportingCollectionManagementService reportingCollectionManagementService) : ControllerBase
{
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
        logger.LogInformation("Received request to start bulk import with sourceType={sourceType} at {requestTime}", sourceType, DateTime.UtcNow);

        // Validate sourceType
        if (sourceType != BlobStorageSources.Internal && sourceType != BlobStorageSources.External)
        {
            logger.LogWarning("Invalid sourceType provided: {sourceType}", sourceType);
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
                return Conflict(new ErrorResponse
                {
                    Message = "Import is already running. Please wait for the current import to complete.",
                    Timestamp = DateTime.UtcNow
                });
            }

            logger.LogInformation("Bulk import started successfully with importId={importId}, sourceType={sourceType}", importId.Value, sourceType);

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
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
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
    public async Task<IActionResult> GetImportSummaries(
        [FromQuery] int skip = 0,
        [FromQuery] int top = 10,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get import summaries with skip={skip}, top={top}", skip, top);

        // Validate parameters
        if (skip < 0)
        {
            logger.LogWarning("Invalid skip parameter: {skip}", skip);
            return BadRequest(new
            {
                Message = "Skip parameter must be greater than or equal to 0.",
                Timestamp = DateTime.UtcNow
            });
        }

        if (top <= 0 || top > 100)
        {
            logger.LogWarning("Invalid top parameter: {top}", top);
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
            return StatusCode(499, new
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
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
}