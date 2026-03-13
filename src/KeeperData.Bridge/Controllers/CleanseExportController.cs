using KeeperData.Core.Reports;
using KeeperData.Core.Reports.Cleanse.Export.Operations.Dtos;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/cleanse-export")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class CleanseExportController(
    ICleanseFacade cleanseFacade,
    ILogger<CleanseExportController> logger) : ControllerBase
{
    /// <summary>
    /// Starts an ad-hoc full export of all active issues.
    /// The export runs in the background and progress can be tracked via the returned export ID.
    /// </summary>
    [HttpPost("start")]
    [ProducesResponseType(typeof(StartExportResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(typeof(ExportErrorResponse), StatusCodes.Status409Conflict)]
    public async Task<IActionResult> StartFullExport(CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to start ad-hoc full export at {RequestTime}", DateTime.UtcNow);

        var operation = await cleanseFacade.Commands.CleanseExportCommandService.StartFullExportAsync(cancellationToken);

        if (operation is null)
        {
            logger.LogWarning("Failed to start full export - another export is already running");
            return Conflict(new ExportErrorResponse
            {
                Message = "An export is already running. Please wait for it to complete.",
                Timestamp = DateTime.UtcNow
            });
        }

        logger.LogInformation("Full export started successfully with exportId={ExportId}", operation.Id);

        return Accepted(new StartExportResponse
        {
            ExportId = operation.Id,
            Status = operation.Status,
            Message = "Full export started successfully and is running in the background.",
            StartedAtUtc = operation.StartedAtUtc
        });
    }

    /// <summary>
    /// Gets the progress and details of a specific export operation.
    /// When the export completes, the response includes the presigned download URL.
    /// </summary>
    [HttpGet("{exportId}")]
    [ProducesResponseType(typeof(CleanseExportOperationDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ExportErrorResponse), StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetExportOperation(string exportId, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get export operation exportId={ExportId}", exportId);

        var operation = await cleanseFacade.Commands.CleanseExportCommandService.GetExportOperationAsync(exportId, cancellationToken);

        if (operation is null)
        {
            return NotFound(new ExportErrorResponse
            {
                Message = $"Export operation not found: {exportId}",
                Timestamp = DateTime.UtcNow
            });
        }

        return Ok(operation);
    }

    /// <summary>
    /// Regenerates the presigned URL for an export operation's report.
    /// Use this when the original presigned URL has expired.
    /// </summary>
    [HttpPost("{exportId}/regenerate-url")]
    [ProducesResponseType(typeof(RegenerateExportUrlResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ExportErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ExportErrorResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> RegenerateExportUrl(string exportId, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to regenerate export URL for exportId={ExportId}", exportId);

        var result = await cleanseFacade.Commands.CleanseExportCommandService.RegenerateExportUrlAsync(exportId, cancellationToken);

        if (!result.Success)
        {
            if (result.Error?.Contains("not found") == true)
            {
                return NotFound(new ExportErrorResponse
                {
                    Message = result.Error,
                    Timestamp = DateTime.UtcNow
                });
            }

            return BadRequest(new ExportErrorResponse
            {
                Message = result.Error ?? "Failed to regenerate export URL.",
                Timestamp = DateTime.UtcNow
            });
        }

        return Ok(new RegenerateExportUrlResponse
        {
            ExportId = result.OperationId!,
            ObjectKey = result.ObjectKey!,
            ReportUrl = result.ReportUrl!,
            RegeneratedAtUtc = DateTime.UtcNow
        });
    }

    /// <summary>
    /// Gets a paginated list of export operations in reverse chronological order.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(ExportOperationsResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ExportErrorResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetExportOperations(
        [FromQuery] int skip = 0,
        [FromQuery] int top = 10,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to list export operations with skip={Skip}, top={Top}", skip, top);

        if (skip < 0)
        {
            return BadRequest(new ExportErrorResponse
            {
                Message = "Skip parameter must be greater than or equal to 0.",
                Timestamp = DateTime.UtcNow
            });
        }

        if (top <= 0 || top > 100)
        {
            return BadRequest(new ExportErrorResponse
            {
                Message = "Top parameter must be between 1 and 100.",
                Timestamp = DateTime.UtcNow
            });
        }

        var exports = await cleanseFacade.Commands.CleanseExportCommandService.GetExportOperationsAsync(skip, top, cancellationToken);

        return Ok(new ExportOperationsResponse
        {
            Skip = skip,
            Top = top,
            Count = exports.Count,
            Exports = exports,
            Timestamp = DateTime.UtcNow
        });
    }
}

#region Response DTOs

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record StartExportResponse
{
    public required string ExportId { get; init; }
    public required string Status { get; init; }
    public required string Message { get; init; }
    public DateTime StartedAtUtc { get; init; }
}

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record RegenerateExportUrlResponse
{
    public required string ExportId { get; init; }
    public required string ObjectKey { get; init; }
    public required string ReportUrl { get; init; }
    public DateTime RegeneratedAtUtc { get; init; }
}

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record ExportOperationsResponse
{
    public int Skip { get; init; }
    public int Top { get; init; }
    public int Count { get; init; }
    public required IReadOnlyList<CleanseExportOperationSummaryDto> Exports { get; init; }
    public DateTime Timestamp { get; init; }
}

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record ExportErrorResponse
{
    public required string Message { get; init; }
    public DateTime Timestamp { get; init; }
}

#endregion
