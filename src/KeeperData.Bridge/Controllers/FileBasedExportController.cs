using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Export;
using KeeperData.Core.ETL.Models;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/etl/file-based/exports")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class FileBasedExportController(
    ICphExportStatusService cphExportStatusService,
    IServiceScopeFactory serviceScopeFactory,
    ILogger<FileBasedExportController> logger) : ControllerBase
{
    /// <summary>
    /// Triggers a CPH export from the latest DuckDB staging file to SQLite.
    /// The export runs in the background; poll GET /api/etl/file-based/exports/{exportId} for progress.
    /// Returns 409 if an export is already queued or running.
    /// </summary>
    [HttpPost("cphs")]
    [ProducesResponseType(typeof(CphExportAcceptedResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(typeof(CphExportErrorResponse), StatusCodes.Status409Conflict)]
    [ProducesResponseType(typeof(CphExportErrorResponse), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> TriggerCphExport(CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to trigger CPH export at {RequestTime}", DateTime.UtcNow);

        try
        {
            var runningExport = await cphExportStatusService.GetLatestRunningAsync(cancellationToken);
            if (runningExport is not null)
            {
                logger.LogWarning("CPH export conflict — export {ExportId} is already {Status}",
                    runningExport.ExportId, runningExport.Status);

                return Conflict(new CphExportErrorResponse
                {
                    Message = $"An export is already {runningExport.Status.ToString().ToLowerInvariant()}. Please wait for it to complete.",
                    ExportId = runningExport.ExportId,
                    Timestamp = DateTime.UtcNow
                });
            }

            var exportId = Guid.NewGuid();
            var status = await cphExportStatusService.CreateAsync(exportId, "staging/latest", cancellationToken);

            _ = Task.Run(() => ExecuteExportInBackground(exportId), CancellationToken.None);

            logger.LogInformation("CPH export {ExportId} accepted and queued for background execution", exportId);

            return Accepted(new CphExportAcceptedResponse
            {
                ExportId = exportId,
                Status = status.Status.ToString(),
                Message = "CPH export queued and will run in the background.",
                RequestedAt = status.RequestedAt
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("CPH export trigger request was cancelled");
            return StatusCode(499, new CphExportErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error triggering CPH export");
            return StatusCode(StatusCodes.Status500InternalServerError, new CphExportErrorResponse
            {
                Message = "An unexpected error occurred while triggering the export.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    private async Task ExecuteExportInBackground(Guid exportId)
    {
        await using var scope = serviceScopeFactory.CreateAsyncScope();
        var scopedExportService = scope.ServiceProvider.GetRequiredService<ICphExportService>();
        var scopedStatusService = scope.ServiceProvider.GetRequiredService<ICphExportStatusService>();
        var scopedLogger = scope.ServiceProvider.GetRequiredService<ILogger<FileBasedExportController>>();

        try
        {
            var status = await scopedStatusService.GetAsync(exportId);
            if (status is null)
            {
                scopedLogger.LogError("Export status {ExportId} not found during background execution", exportId);
                return;
            }

            status.Status = ExportStatusType.Running;
            status.StartedAt = DateTime.UtcNow;
            await scopedStatusService.UpdateAsync(status);

            scopedLogger.LogInformation("CPH export {ExportId} started running", exportId);

            var result = await scopedExportService.ExportAsync();

            status.Status = ExportStatusType.Succeeded;
            status.CompletedAt = DateTime.UtcNow;
            status.SqlitePath = result.SqliteKey;
            status.RowCount = result.RowCount;
            await scopedStatusService.UpdateAsync(status);

            scopedLogger.LogInformation("CPH export {ExportId} succeeded — {RowCount} rows exported to {SqliteKey}",
                exportId, result.RowCount, result.SqliteKey);
        }
        catch (Exception ex)
        {
            scopedLogger.LogError(ex, "CPH export {ExportId} failed", exportId);

            try
            {
                var status = await scopedStatusService.GetAsync(exportId);
                if (status is not null)
                {
                    status.Status = ExportStatusType.Failed;
                    status.CompletedAt = DateTime.UtcNow;
                    status.ErrorMessage = ex.Message;
                    await scopedStatusService.UpdateAsync(status);
                }
            }
            catch (Exception updateEx)
            {
                scopedLogger.LogError(updateEx, "Failed to update export status {ExportId} after error", exportId);
            }
        }
    }
}

#region Response DTOs

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record CphExportAcceptedResponse
{
    public required Guid ExportId { get; init; }
    public required string Status { get; init; }
    public required string Message { get; init; }
    public DateTime RequestedAt { get; init; }
}

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record CphExportErrorResponse
{
    public required string Message { get; init; }
    public Guid? ExportId { get; init; }
    public DateTime Timestamp { get; init; }
}

#endregion
