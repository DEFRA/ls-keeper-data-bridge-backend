using KeeperData.Bridge.Worker.Tasks;
using KeeperData.Infrastructure.Storage;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/[controller]")]
public class ImportController(
    ITaskProcessBulkFiles taskProcessBulkFiles,
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
        catch (Exception ex)
        {
            logger.LogError(ex, "Error occurred while starting bulk import");
            return StatusCode(500, new
            {
                Message = "An error occurred while starting the import process.",
                Error = ex.Message,
                Timestamp = DateTime.UtcNow
            });
        }
    }
}
