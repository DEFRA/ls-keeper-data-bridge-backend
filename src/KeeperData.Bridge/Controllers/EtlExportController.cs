using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Export;
using KeeperData.Core.ETL.Models;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/etl/exports")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class EtlExportController(
    ICphExportStatusService cphExportStatusService,
    IServiceScopeFactory serviceScopeFactory,
    ILogger<EtlExportController> logger) : ControllerBase
{
    /// <summary>
    /// Triggers a CPH export from the latest DuckDB staging file to SQLite.
    /// The export runs in the background; poll GET /api/etl/exports/{exportId} for progress.
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

                return BuildConflictResponse(runningExport);
            }

            var exportId = Guid.NewGuid();
            var status = await cphExportStatusService.CreateAsync(exportId, "staging/latest", cancellationToken);

            _ = Task.Run(() => ExecuteExportInBackground(exportId), CancellationToken.None);

            logger.LogInformation("CPH export {ExportId} accepted and queued for background execution", exportId);

            return BuildAcceptedResponse(exportId, status);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("CPH export trigger request was cancelled");
            return CancelledResponse();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error triggering CPH export");
            return InternalErrorResponse("An unexpected error occurred while triggering the export.");
        }
    }

    /// <summary>
    /// Gets the current status of a CPH export operation.
    /// Returns the full status including progress, result path, and any error details.
    /// </summary>
    /// <param name="exportId">The export ID returned by the trigger endpoint</param>
    /// <param name="cancellationToken">Cancellation token</param>
    [HttpGet("{exportId:guid}")]
    [ProducesResponseType(typeof(CphExportStatusResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(CphExportErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(CphExportErrorResponse), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> GetExportStatus(Guid exportId, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get export status for {ExportId}", exportId);

        try
        {
            var status = await cphExportStatusService.GetAsync(exportId, cancellationToken);

            if (status is null)
            {
                logger.LogWarning("Export status not found for {ExportId}", exportId);
                return NotFound(ErrorResponse($"Export not found: {exportId}", exportId));
            }

            return Ok(MapToStatusResponse(status));
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get export status request was cancelled for {ExportId}", exportId);
            return CancelledResponse();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error getting export status for {ExportId}", exportId);
            return InternalErrorResponse("An unexpected error occurred while retrieving the export status.");
        }
    }

    private static CphExportErrorResponse ErrorResponse(string message, Guid? exportId = null) =>
        new() { Message = message, ExportId = exportId, Timestamp = DateTime.UtcNow };

    private IActionResult BuildConflictResponse(CphExportStatus runningExport) =>
        Conflict(ErrorResponse(
            $"An export is already {runningExport.Status.ToString().ToLowerInvariant()}. Please wait for it to complete.",
            runningExport.ExportId));

    private IActionResult BuildAcceptedResponse(Guid exportId, CphExportStatus status) =>
        Accepted(new CphExportAcceptedResponse
        {
            ExportId = exportId,
            Status = status.Status.ToString(),
            Message = "CPH export queued and will run in the background.",
            RequestedAt = status.RequestedAt
        });

    private IActionResult CancelledResponse() =>
        StatusCode(499, ErrorResponse("Request was cancelled."));

    private IActionResult InternalErrorResponse(string message) =>
        StatusCode(StatusCodes.Status500InternalServerError, ErrorResponse(message));

    private static CphExportStatusResponse MapToStatusResponse(CphExportStatus status) => new()
    {
        ExportId = status.ExportId,
        Status = status.Status.ToString(),
        RequestedAt = status.RequestedAt,
        StartedAt = status.StartedAt,
        CompletedAt = status.CompletedAt,
        SourceDuckDbPath = status.SourceDuckDbPath,
        SqlitePath = status.SqlitePath,
        RowCount = status.RowCount,
        ErrorMessage = status.ErrorMessage
    };

    private async Task ExecuteExportInBackground(Guid exportId)
    {
        await using var scope = serviceScopeFactory.CreateAsyncScope();
        var (scopedExportService, scopedStatusService, scopedLogger) = GetScopedServices(scope);

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

            await UpdateSuccessStatus(scopedStatusService, status, result);

            scopedLogger.LogInformation("CPH export {ExportId} succeeded — {RowCount} rows exported to {SqliteKey}",
                exportId, result.RowCount, result.SqliteKey);
        }
        catch (Exception ex)
        {
            scopedLogger.LogError(ex, "CPH export {ExportId} failed", exportId);
            await UpdateExceptionStatus(scopedStatusService, exportId, ex, scopedLogger);
        }
    }

    private static async Task UpdateExceptionStatus(ICphExportStatusService scopedStatusService, Guid exportId, Exception ex, ILogger<EtlExportController> scopedLogger)
    {
        try
        {
            await UpdateStatusAsync(scopedStatusService, exportId, s =>
            {
                s.Status = ExportStatusType.Failed;
                s.CompletedAt = DateTime.UtcNow;
                s.ErrorMessage = ex.Message;
            });
        }
        catch (Exception updateEx)
        {
            scopedLogger.LogError(updateEx, "Failed to update export status {ExportId} after error", exportId);
        }
    }

    private static (ICphExportService, ICphExportStatusService, ILogger<EtlExportController>) GetScopedServices(
        IServiceScope scope) => (
            scope.ServiceProvider.GetRequiredService<ICphExportService>(),
            scope.ServiceProvider.GetRequiredService<ICphExportStatusService>(),
            scope.ServiceProvider.GetRequiredService<ILogger<EtlExportController>>());

    private static async Task UpdateSuccessStatus(
        ICphExportStatusService statusService,
        CphExportStatus status,
        CphExportResult result)
    {
        status.Status = ExportStatusType.Succeeded;
        status.CompletedAt = DateTime.UtcNow;
        status.SqlitePath = result.SqliteKey;
        status.RowCount = result.RowCount;
        await statusService.UpdateAsync(status);
    }

    private static async Task UpdateStatusAsync(
        ICphExportStatusService statusService,
        Guid exportId,
        Action<CphExportStatus> mutate)
    {
        var status = await statusService.GetAsync(exportId);
        if (status is not null)
        {
            mutate(status);
            await statusService.UpdateAsync(status);
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
public record CphExportStatusResponse
{
    public required Guid ExportId { get; init; }
    public required string Status { get; init; }
    public required DateTime RequestedAt { get; init; }
    public DateTime? StartedAt { get; init; }
    public DateTime? CompletedAt { get; init; }
    public required string SourceDuckDbPath { get; init; }
    public string? SqlitePath { get; init; }
    public int? RowCount { get; init; }
    public string? ErrorMessage { get; init; }
}

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record CphExportErrorResponse
{
    public required string Message { get; init; }
    public Guid? ExportId { get; init; }
    public DateTime Timestamp { get; init; }
}

#endregion
