using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Dtos;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/[controller]")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
[System.Diagnostics.CodeAnalysis.SuppressMessage("SonarQube", "S6960", Justification = "Controller endpoints are cohesively related to cleanse operations")]
public class CleanseController(
    ICleanseReportService cleanseReportService,
    ICleanseReportNotificationService notificationService,
    ILogger<CleanseController> logger) : ControllerBase
{
    /// <summary>
    /// Starts a new cleanse analysis operation.
    /// The analysis runs in the background and progress can be tracked via the returned operation ID.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The started operation details, or 409 if an analysis is already running</returns>
    [HttpPost("start-analysis")]
    [ProducesResponseType(typeof(StartAnalysisResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status409Conflict)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> StartAnalysis(CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to start cleanse analysis at {RequestTime}", DateTime.UtcNow);

        try
        {
            var operation = await cleanseReportService.StartAnalysisAsync(cancellationToken);

            if (operation is null)
            {
                logger.LogWarning("Failed to start cleanse analysis - another analysis is already running");
                return Conflict(new ErrorResponse
                {
                    Message = "A cleanse analysis is already running. Please wait for it to complete.",
                    Timestamp = DateTime.UtcNow
                });
            }

            logger.LogInformation("Cleanse analysis started successfully with operationId={OperationId}", operation.Id);

            return Accepted(new StartAnalysisResponse
            {
                OperationId = operation.Id,
                Status = operation.Status.ToString(),
                Message = "Cleanse analysis started successfully and is running in the background.",
                StartedAtUtc = operation.StartedAtUtc
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Start analysis request was cancelled");
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Deletes all cleanse report data (detected issues).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result of the delete operation</returns>
    [HttpPost("delete-data")]
    [ProducesResponseType(typeof(DeleteDataResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status500InternalServerError)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> DeleteData(CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to delete cleanse report data");

        try
        {
            var result = await cleanseReportService.DeleteReportDataAsync(cancellationToken);

            if (!result.Success)
            {
                logger.LogError("Failed to delete cleanse report data: {Message}", result.Message);
                return StatusCode(500, new ErrorResponse
                {
                    Message = result.Message,
                    Timestamp = DateTime.UtcNow
                });
            }

            logger.LogInformation("Successfully deleted {DeletedCount} cleanse report items", result.DeletedCount);

            return Ok(new DeleteDataResponse
            {
                Success = true,
                CollectionName = result.CollectionName,
                DeletedCount = result.DeletedCount ?? 0,
                Message = result.Message,
                DeletedAtUtc = result.OperatedAtUtc
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Delete data request was cancelled");
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Deletes all cleanse analysis metadata (operation history).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result of the delete operation</returns>
    [HttpPost("delete-metadata")]
    [ProducesResponseType(typeof(DeleteDataResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status500InternalServerError)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> DeleteMetadata(CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to delete cleanse analysis metadata");

        try
        {
            var result = await cleanseReportService.DeleteMetadataAsync(cancellationToken);

            if (!result.Success)
            {
                logger.LogError("Failed to delete cleanse analysis metadata: {Message}", result.Message);
                return StatusCode(500, new ErrorResponse
                {
                    Message = result.Message,
                    Timestamp = DateTime.UtcNow
                });
            }

            logger.LogInformation("Successfully deleted {DeletedCount} analysis operation records", result.DeletedCount);

            return Ok(new DeleteDataResponse
            {
                Success = true,
                CollectionName = result.CollectionName,
                DeletedCount = result.DeletedCount ?? 0,
                Message = result.Message,
                DeletedAtUtc = result.OperatedAtUtc
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Delete metadata request was cancelled");
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Gets a paginated list of analysis run summaries in reverse chronological order.
    /// </summary>
    /// <param name="skip">Number of records to skip (default: 0)</param>
    /// <param name="top">Maximum number of records to return (default: 10, max: 100)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Paginated list of analysis run summaries</returns>
    [HttpGet("runs")]
    [ProducesResponseType(typeof(AnalysisRunsResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> GetRuns(
        [FromQuery] int skip = 0,
        [FromQuery] int top = 10,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get analysis runs with skip={Skip}, top={Top}", skip, top);

        if (skip < 0)
        {
            return BadRequest(new ErrorResponse
            {
                Message = "Skip parameter must be greater than or equal to 0.",
                Timestamp = DateTime.UtcNow
            });
        }

        if (top <= 0 || top > 100)
        {
            return BadRequest(new ErrorResponse
            {
                Message = "Top parameter must be between 1 and 100.",
                Timestamp = DateTime.UtcNow
            });
        }

        try
        {
            var runs = await cleanseReportService.GetOperationsAsync(skip, top, cancellationToken);

            logger.LogInformation("Successfully retrieved {Count} analysis runs", runs.Count);

            return Ok(new AnalysisRunsResponse
            {
                Skip = skip,
                Top = top,
                Count = runs.Count,
                Runs = runs,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get runs request was cancelled");
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Gets the full details of a specific analysis run.
    /// </summary>
    /// <param name="operationId">The operation ID of the analysis run</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Full details of the analysis run, or 404 if not found</returns>
    [HttpGet("run/{operationId}")]
    [ProducesResponseType(typeof(CleanseAnalysisOperation), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> GetRun(string operationId, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get analysis run for operationId={OperationId}", operationId);

        try
        {
            var operation = await cleanseReportService.GetOperationAsync(operationId, cancellationToken);

            if (operation is null)
            {
                logger.LogWarning("Analysis run not found for operationId={OperationId}", operationId);
                return NotFound(new ErrorResponse
                {
                    Message = $"Analysis run not found for OperationId: {operationId}",
                    Timestamp = DateTime.UtcNow
                });
            }

            logger.LogInformation("Successfully retrieved analysis run for operationId={OperationId}", operationId);

            return Ok(operation);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get run request was cancelled for operationId={OperationId}", operationId);
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Gets a paginated list of current active cleanse issues.
    /// </summary>
    /// <param name="skip">Number of records to skip (default: 0)</param>
    /// <param name="top">Maximum number of records to return (default: 50, max: 100)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Paginated list of active cleanse issues</returns>
    [HttpGet("issues")]
    [ProducesResponseType(typeof(IssuesResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> GetIssues(
        [FromQuery] int skip = 0,
        [FromQuery] int top = 50,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get issues with skip={Skip}, top={Top}", skip, top);

        if (skip < 0)
        {
            return BadRequest(new ErrorResponse
            {
                Message = "Skip parameter must be greater than or equal to 0.",
                Timestamp = DateTime.UtcNow
            });
        }

        if (top <= 0 || top > 100)
        {
            return BadRequest(new ErrorResponse
            {
                Message = "Top parameter must be between 1 and 100.",
                Timestamp = DateTime.UtcNow
            });
        }

        try
        {
            var result = await cleanseReportService.ListIssuesAsync(skip, top, cancellationToken);

            logger.LogInformation("Successfully retrieved {Count} issues (total: {TotalCount})", result.Items.Count, result.TotalCount);

            return Ok(new IssuesResponse
            {
                Skip = result.Skip,
                Top = result.Top,
                Count = result.Items.Count,
                TotalCount = result.TotalCount,
                Issues = result.Items,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get issues request was cancelled");
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Regenerates the presigned URL for an operation's report.
    /// Use this when the original presigned URL has expired.
    /// </summary>
    /// <param name="operationId">The operation ID</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The regenerated presigned URL</returns>
    [HttpPost("run/{operationId}/regenerate-url")]
    [ProducesResponseType(typeof(RegenerateUrlResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> RegenerateReportUrl(string operationId, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to regenerate report URL for operationId={OperationId}", operationId);

        try
        {
            var result = await cleanseReportService.RegenerateReportUrlAsync(operationId, cancellationToken);

            if (!result.Success)
            {
                if (result.Error?.Contains("not found") == true)
                {
                    return NotFound(new ErrorResponse
                    {
                        Message = result.Error,
                        Timestamp = DateTime.UtcNow
                    });
                }

                return BadRequest(new ErrorResponse
                {
                    Message = result.Error ?? "Failed to regenerate report URL.",
                    Timestamp = DateTime.UtcNow
                });
            }

            logger.LogInformation(
                "Successfully regenerated report URL for operationId={OperationId}. New URL: {ReportUrl}",
                operationId, result.ReportUrl);

            return Ok(new RegenerateUrlResponse
            {
                OperationId = result.OperationId!,
                ObjectKey = result.ObjectKey!,
                ReportUrl = result.ReportUrl!,
                RegeneratedAtUtc = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Regenerate URL request was cancelled for operationId={OperationId}", operationId);
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Sends a test notification email to verify GOV.UK Notify configuration.
    /// The test email is sent to test@example.com using the configured template.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result indicating whether the test email was sent successfully</returns>
    [HttpPost("test-notification")]
    [ProducesResponseType(typeof(TestNotificationResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status500InternalServerError)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> SendTestNotification(CancellationToken cancellationToken = default)
    {
        const string testEmailAddress = "test@example.com";

        logger.LogInformation("Received request to send test notification to {TestEmail}", testEmailAddress);

        try
        {
            var result = await notificationService.SendTestNotificationAsync(testEmailAddress, cancellationToken);

            if (!result.Success)
            {
                logger.LogError(
                    "Failed to send test notification to {TestEmail}. Error: {Error}",
                    testEmailAddress, result.Error);

                return StatusCode(500, new ErrorResponse
                {
                    Message = $"Failed to send test notification: {result.Error}",
                    Timestamp = DateTime.UtcNow
                });
            }

            logger.LogInformation(
                "Successfully sent test notification to {TestEmail}. NotificationId: {NotificationId}",
                testEmailAddress, result.NotificationId);

            return Ok(new TestNotificationResponse
            {
                Success = true,
                Recipient = result.Recipient!,
                NotificationId = result.NotificationId,
                Message = "Test notification sent successfully.",
                SentAtUtc = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Test notification request was cancelled");
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error when sending test notification to {TestEmail}", testEmailAddress);
            return StatusCode(500, new ErrorResponse
            {
                Message = $"Unexpected error: {ex.Message}",
                Timestamp = DateTime.UtcNow
            });
        }
    }
}

#region Response DTOs

/// <summary>
/// Response when a cleanse analysis is successfully started.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record StartAnalysisResponse
{
    /// <summary>
    /// Gets the unique identifier for the started analysis operation.
    /// </summary>
    public required string OperationId { get; init; }

    /// <summary>
    /// Gets the current status of the operation.
    /// </summary>
    public required string Status { get; init; }

    /// <summary>
    /// Gets the success message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the analysis was started.
    /// </summary>
    public DateTime StartedAtUtc { get; init; }
}

/// <summary>
/// Response when a report URL is regenerated.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record RegenerateUrlResponse
{
    /// <summary>
    /// Gets the operation ID.
    /// </summary>
    public required string OperationId { get; init; }

    /// <summary>
    /// Gets the S3 object key.
    /// </summary>
    public required string ObjectKey { get; init; }

    /// <summary>
    /// Gets the new presigned URL.
    /// </summary>
    public required string ReportUrl { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the URL was regenerated.
    /// </summary>
    public DateTime RegeneratedAtUtc { get; init; }
}

/// <summary>
/// Response when cleanse data is deleted.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record DeleteDataResponse
{
    /// <summary>
    /// Gets a value indicating whether the deletion was successful.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Gets the name of the affected collection.
    /// </summary>
    public required string CollectionName { get; init; }

    /// <summary>
    /// Gets the number of documents deleted.
    /// </summary>
    public long DeletedCount { get; init; }

    /// <summary>
    /// Gets the result message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the deletion was performed.
    /// </summary>
    public DateTime DeletedAtUtc { get; init; }
}

/// <summary>
/// Response for paginated analysis runs.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record AnalysisRunsResponse
{
    /// <summary>
    /// Gets the number of records skipped.
    /// </summary>
    public int Skip { get; init; }

    /// <summary>
    /// Gets the maximum number of records requested.
    /// </summary>
    public int Top { get; init; }

    /// <summary>
    /// Gets the actual number of runs returned.
    /// </summary>
    public int Count { get; init; }

    /// <summary>
    /// Gets the list of analysis run summaries.
    /// </summary>
    public required IReadOnlyList<CleanseAnalysisOperationSummary> Runs { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the response.
    /// </summary>
    public DateTime Timestamp { get; init; }
}

/// <summary>
/// Response for paginated cleanse issues.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record IssuesResponse
{
    /// <summary>
    /// Gets the number of records skipped.
    /// </summary>
    public int Skip { get; init; }

    /// <summary>
    /// Gets the maximum number of records requested.
    /// </summary>
    public int Top { get; init; }

    /// <summary>
    /// Gets the actual number of issues returned.
    /// </summary>
    public int Count { get; init; }

    /// <summary>
    /// Gets the total count of active issues.
    /// </summary>
    public int TotalCount { get; init; }

    /// <summary>
    /// Gets the list of cleanse issues.
    /// </summary>
    public required IReadOnlyList<CleanseReportItem> Issues { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the response.
    /// </summary>
    public DateTime Timestamp { get; init; }
}

/// <summary>
/// Standard error response.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record ErrorResponse
{
    /// <summary>
    /// Gets the error message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the error.
    /// </summary>
    public DateTime Timestamp { get; init; }
}

/// <summary>
/// Response for test notification request.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record TestNotificationResponse
{
    /// <summary>
    /// Gets a value indicating whether the test notification was sent successfully.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Gets the recipient email address.
    /// </summary>
    public required string Recipient { get; init; }

    /// <summary>
    /// Gets the notification ID from GOV.UK Notify.
    /// </summary>
    public string? NotificationId { get; init; }

    /// <summary>
    /// Gets a message describing the result.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the notification was sent.
    /// </summary>
    public DateTime SentAtUtc { get; init; }
}

#endregion
