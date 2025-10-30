using KeeperData.Bridge.Worker.Tasks;
using KeeperData.Core.Reporting;
using KeeperData.Infrastructure.Storage;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/[controller]")]
public class ImportController(
    ITaskProcessBulkFiles taskProcessBulkFiles,
    IImportReportingService importReportingService,
    ILogger<ImportController> logger) : ControllerBase
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
    public async Task<IActionResult> StartBulkImport([FromQuery] string sourceType = BlobStorageSources.External, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to start bulk import with sourceType={sourceType} at {requestTime}", sourceType, DateTime.UtcNow);

        // Validate sourceType
        if (sourceType != BlobStorageSources.Internal && sourceType != BlobStorageSources.External)
        {
            logger.LogWarning("Invalid sourceType provided: {sourceType}", sourceType);
            return BadRequest(new
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
                return Conflict(new
                {
                    Message = "Import is already running. Please wait for the current import to complete.",
                    Timestamp = DateTime.UtcNow
                });
            }

            logger.LogInformation("Bulk import started successfully with importId={importId}, sourceType={sourceType}", importId.Value, sourceType);

            return Accepted(new
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
            return StatusCode(499, new
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
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
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

            return Ok(new
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
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
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
    /// <param name="importId">The import ID to retrieve file reports for</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>List of file processing reports</returns>
    [HttpGet("{importId}/files")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
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

            return Ok(new
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
}