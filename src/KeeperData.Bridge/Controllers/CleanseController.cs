using KeeperData.Core.Reports;
using KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using KeeperData.Core.Reports.Issues.Command.Requests;
using KeeperData.Core.Reports.Issues.Query.Dtos;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/[controller]")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
[SuppressMessage("SonarQube", "S6960", Justification = "Controller endpoints are cohesively related to cleanse operations")]
public class CleanseController(
    ICleanseFacade cleanseFacade,
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
            var operation = await cleanseFacade.Commands.CleanseAnalysisCommandService.StartAnalysisAsync(cancellationToken);

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
            var deletedCount = await cleanseFacade.Commands.IssueCommandService.DeleteAllIssuesAsync(cancellationToken);

            logger.LogInformation("Successfully deleted {DeletedCount} cleanse report items", deletedCount);

            return Ok(new DeleteDataResponse
            {
                Success = true,
                CollectionName = "",
                DeletedCount = deletedCount,
                Message = $"Successfully deleted {deletedCount} cleanse report items.",
                DeletedAtUtc = DateTime.UtcNow
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
            var deletedCount = await cleanseFacade.Commands.CleanseOperationCommandService.DeleteMetadataAsync(cancellationToken);

            logger.LogInformation("Successfully deleted {DeletedCount} analysis operation records", deletedCount);

            return Ok(new DeleteDataResponse
            {
                Success = true,
                CollectionName = "",
                DeletedCount = deletedCount,
                Message = $"Successfully deleted {deletedCount} analysis operation records.",
                DeletedAtUtc = DateTime.UtcNow
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
            var runs = await cleanseFacade.Queries.CleanseAnalysisOperationsQueries.GetOperationsAsync(skip, top, cancellationToken);

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
    [ProducesResponseType(typeof(CleanseAnalysisOperationDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> GetRun(string operationId, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get analysis run for operationId={OperationId}", operationId);

        try
        {
            var operation = await cleanseFacade.Queries.CleanseAnalysisOperationsQueries.GetOperationAsync(operationId, cancellationToken);

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
    /// Gets a paginated, filterable, and sortable list of cleanse issues.
    /// </summary>
    /// <param name="skip">Number of records to skip (default: 0)</param>
    /// <param name="top">Maximum number of records to return (default: 50, max: 100)</param>
    /// <param name="ctsLidFullIdentifier">Filter by CTS LID full identifier (contains, case-insensitive)</param>
    /// <param name="cph">Filter by CPH (contains, case-insensitive)</param>
    /// <param name="issueCode">Filter by exact issue code</param>
    /// <param name="ruleCode">Filter by exact rule code</param>
    /// <param name="errorCode">Filter by exact error code</param>
    /// <param name="isActive">Filter by active status</param>
    /// <param name="isIgnored">Filter by ignored status</param>
    /// <param name="resolutionStatus">Filter by resolution status (None, Todo, InProgress, Resolved)</param>
    /// <param name="assignedTo">Filter by assigned user</param>
    /// <param name="isUnassigned">Filter to unassigned issues only</param>
    /// <param name="createdAfterUtc">Filter issues created after this UTC time</param>
    /// <param name="createdBeforeUtc">Filter issues created before this UTC time</param>
    /// <param name="updatedAfterUtc">Filter issues updated after this UTC time</param>
    /// <param name="updatedBeforeUtc">Filter issues updated before this UTC time</param>
    /// <param name="sortBy">Field to sort by (default: LastUpdatedAtUtc)</param>
    /// <param name="sortDescending">Sort in descending order (default: true)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Paginated list of cleanse issues matching the filters</returns>
    [HttpGet("issues")]
    [ProducesResponseType(typeof(IssuesResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> GetIssues(
        [FromQuery] int skip = 0,
        [FromQuery] int top = 50,
        [FromQuery] string? ctsLidFullIdentifier = null,
        [FromQuery] string? cph = null,
        [FromQuery] string? issueCode = null,
        [FromQuery] string? ruleCode = null,
        [FromQuery] string? errorCode = null,
        [FromQuery] bool? isActive = null,
        [FromQuery] bool? isIgnored = null,
        [FromQuery] string? resolutionStatus = null,
        [FromQuery] string? assignedTo = null,
        [FromQuery] bool? isUnassigned = null,
        [FromQuery] DateTime? createdAfterUtc = null,
        [FromQuery] DateTime? createdBeforeUtc = null,
        [FromQuery] DateTime? updatedAfterUtc = null,
        [FromQuery] DateTime? updatedBeforeUtc = null,
        [FromQuery] string? sortBy = null,
        [FromQuery] bool sortDescending = true,
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

        CleanseIssueSortField sortField = CleanseIssueSortField.LastUpdatedAtUtc;
        if (!string.IsNullOrEmpty(sortBy) && !Enum.TryParse(sortBy, ignoreCase: true, out sortField))
        {
            return BadRequest(new ErrorResponse
            {
                Message = $"Invalid sortBy value '{sortBy}'. Valid values are: {string.Join(", ", Enum.GetNames<CleanseIssueSortField>())}.",
                Timestamp = DateTime.UtcNow
            });
        }

        try
        {
            var query = CleanseIssueQueryDto.Create()
                .OrderBy(sortField, sortDescending)
                .Page(skip, top);

            if (isActive.HasValue)
            {
                if (isActive.Value) query.WhereActive(); else query.WhereInactive();
            }

            if (!string.IsNullOrEmpty(ctsLidFullIdentifier)) query.WithCtsLidFullIdentifierContaining(ctsLidFullIdentifier);
            if (!string.IsNullOrEmpty(cph)) query.WithCphContaining(cph);
            if (!string.IsNullOrEmpty(issueCode)) query.WithIssueCode(issueCode);
            if (!string.IsNullOrEmpty(ruleCode)) query.WithRuleCode(ruleCode);
            if (!string.IsNullOrEmpty(errorCode)) query.WithErrorCode(errorCode);
            if (isIgnored.HasValue)
            {
                if (isIgnored.Value) query.WhereIgnored(); else query.WhereNotIgnored();
            }
            if (!string.IsNullOrEmpty(resolutionStatus)) query.WithResolutionStatus(resolutionStatus);
            if (!string.IsNullOrEmpty(assignedTo)) query.WithAssignedTo(assignedTo);
            if (isUnassigned == true) query.WhereUnassigned();
            if (createdAfterUtc.HasValue) query.CreatedAfter(createdAfterUtc.Value);
            if (createdBeforeUtc.HasValue) query.CreatedBefore(createdBeforeUtc.Value);
            if (updatedAfterUtc.HasValue) query.UpdatedAfter(updatedAfterUtc.Value);
            if (updatedBeforeUtc.HasValue) query.UpdatedBefore(updatedBeforeUtc.Value);

            var result = await cleanseFacade.Queries.IssueQueries.QueryAsync(query, cancellationToken);

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
            var result = await cleanseFacade.Commands.CleanseReportExportCommandService.RegenerateReportUrlAsync(operationId, cancellationToken);

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

    /// <summary>
    /// Flags an issue as ignored.
    /// </summary>
    /// <param name="issueId">The issue ID</param>
    /// <param name="request">The request containing the performing user</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>200 on success, 404 if the issue is not found</returns>
    [HttpPost("issues/{issueId}/ignore")]
    [ProducesResponseType(typeof(IssueCommandResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> IgnoreIssue(string issueId, [FromBody] PerformedByRequest request, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to ignore issue issueId={IssueId} by {PerformedBy}", issueId, request.PerformedBy);

        try
        {
            await cleanseFacade.Commands.IssueCommandService.IgnoreIssueAsync(
                new IgnoreIssueCommand(issueId, request.PerformedBy), cancellationToken);

            logger.LogInformation("Successfully ignored issue issueId={IssueId}", issueId);

            return Ok(new IssueCommandResponse
            {
                IssueId = issueId,
                Message = "Issue ignored successfully.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning("Issue not found for ignore: issueId={IssueId}", issueId);
            return NotFound(new ErrorResponse
            {
                Message = ex.Message,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Ignore issue request was cancelled for issueId={IssueId}", issueId);
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Removes the ignored flag from an issue.
    /// </summary>
    /// <param name="issueId">The issue ID</param>
    /// <param name="request">The request containing the performing user</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>200 on success, 404 if the issue is not found</returns>
    [HttpPost("issues/{issueId}/unignore")]
    [ProducesResponseType(typeof(IssueCommandResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> UnignoreIssue(string issueId, [FromBody] PerformedByRequest request, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to unignore issue issueId={IssueId} by {PerformedBy}", issueId, request.PerformedBy);

        try
        {
            await cleanseFacade.Commands.IssueCommandService.UnignoreIssueAsync(
                new UnignoreIssueCommand(issueId, request.PerformedBy), cancellationToken);

            logger.LogInformation("Successfully unignored issue issueId={IssueId}", issueId);

            return Ok(new IssueCommandResponse
            {
                IssueId = issueId,
                Message = "Issue unignored successfully.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning("Issue not found for unignore: issueId={IssueId}", issueId);
            return NotFound(new ErrorResponse
            {
                Message = ex.Message,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Unignore issue request was cancelled for issueId={IssueId}", issueId);
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Updates the resolution workflow status of an issue.
    /// </summary>
    /// <param name="issueId">The issue ID</param>
    /// <param name="request">The request containing the new status and performing user</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>200 on success, 404 if the issue is not found, 400 if the status is invalid</returns>
    [HttpPost("issues/{issueId}/resolution-status")]
    [ProducesResponseType(typeof(IssueCommandResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> UpdateResolutionStatus(string issueId, [FromBody] UpdateResolutionStatusRequest request, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to update resolution status for issueId={IssueId} to {Status} by {PerformedBy}", issueId, request.Status, request.PerformedBy);

        if (!Enum.TryParse<ResolutionStatus>(request.Status, ignoreCase: true, out var status))
        {
            return BadRequest(new ErrorResponse
            {
                Message = $"Invalid resolution status '{request.Status}'. Valid values are: {string.Join(", ", Enum.GetNames<ResolutionStatus>())}.",
                Timestamp = DateTime.UtcNow
            });
        }

        try
        {
            await cleanseFacade.Commands.IssueCommandService.UpdateResolutionStatusAsync(
                new UpdateResolutionStatusCommand(issueId, status, request.PerformedBy), cancellationToken);

            logger.LogInformation("Successfully updated resolution status for issueId={IssueId} to {Status}", issueId, status);

            return Ok(new IssueCommandResponse
            {
                IssueId = issueId,
                Message = $"Resolution status updated to '{status}'.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning("Issue not found for resolution status update: issueId={IssueId}", issueId);
            return NotFound(new ErrorResponse
            {
                Message = ex.Message,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Update resolution status request was cancelled for issueId={IssueId}", issueId);
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Assigns an issue to a user.
    /// </summary>
    /// <param name="issueId">The issue ID</param>
    /// <param name="request">The request containing the assignee and performing user</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>200 on success, 404 if the issue is not found</returns>
    [HttpPost("issues/{issueId}/assign")]
    [ProducesResponseType(typeof(IssueCommandResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> AssignIssue(string issueId, [FromBody] AssignIssueRequest request, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to assign issue issueId={IssueId} to {AssignedTo} by {PerformedBy}", issueId, request.AssignedTo, request.PerformedBy);

        try
        {
            await cleanseFacade.Commands.IssueCommandService.AssignIssueAsync(
                new AssignIssueCommand(issueId, request.AssignedTo, request.PerformedBy), cancellationToken);

            logger.LogInformation("Successfully assigned issue issueId={IssueId} to {AssignedTo}", issueId, request.AssignedTo);

            return Ok(new IssueCommandResponse
            {
                IssueId = issueId,
                Message = $"Issue assigned to '{request.AssignedTo}'.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning("Issue not found for assign: issueId={IssueId}", issueId);
            return NotFound(new ErrorResponse
            {
                Message = ex.Message,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Assign issue request was cancelled for issueId={IssueId}", issueId);
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Clears the assigned user from an issue.
    /// </summary>
    /// <param name="issueId">The issue ID</param>
    /// <param name="request">The request containing the performing user</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>200 on success, 404 if the issue is not found</returns>
    [HttpPost("issues/{issueId}/unassign")]
    [ProducesResponseType(typeof(IssueCommandResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> UnassignIssue(string issueId, [FromBody] PerformedByRequest request, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to unassign issue issueId={IssueId} by {PerformedBy}", issueId, request.PerformedBy);

        try
        {
            await cleanseFacade.Commands.IssueCommandService.UnassignIssueAsync(
                new UnassignIssueCommand(issueId, request.PerformedBy), cancellationToken);

            logger.LogInformation("Successfully unassigned issue issueId={IssueId}", issueId);

            return Ok(new IssueCommandResponse
            {
                IssueId = issueId,
                Message = "Issue unassigned successfully.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (InvalidOperationException ex)
        {
            logger.LogWarning("Issue not found for unassign: issueId={IssueId}", issueId);
            return NotFound(new ErrorResponse
            {
                Message = ex.Message,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Unassign issue request was cancelled for issueId={IssueId}", issueId);
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    /// <summary>
    /// Gets a paginated list of history/lineage entries for a specific issue.
    /// </summary>
    /// <param name="issueId">The issue ID</param>
    /// <param name="skip">Number of records to skip (default: 0)</param>
    /// <param name="top">Maximum number of records to return (default: 50, max: 100)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Paginated list of issue history entries</returns>
    [HttpGet("issues/{issueId}/history")]
    [ProducesResponseType(typeof(IssueHistoryResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> GetIssueHistory(
        string issueId,
        [FromQuery] int skip = 0,
        [FromQuery] int top = 50,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get history for issueId={IssueId} with skip={Skip}, top={Top}", issueId, skip, top);

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
            var entries = await cleanseFacade.Queries.IssueQueries.GetIssueHistoryAsync(issueId, skip, top, cancellationToken);

            logger.LogInformation("Successfully retrieved {Count} history entries for issueId={IssueId}", entries.Count, issueId);

            return Ok(new IssueHistoryResponse
            {
                IssueId = issueId,
                Skip = skip,
                Top = top,
                Count = entries.Count,
                Entries = entries,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get issue history request was cancelled for issueId={IssueId}", issueId);
            return StatusCode(499, new ErrorResponse
            {
                Message = "Request was cancelled.",
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
    public required IReadOnlyList<CleanseAnalysisOperationSummaryDto> Runs { get; init; }

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
    public required IReadOnlyList<IssueDto> Issues { get; init; }

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

/// <summary>
/// Request body containing only the performing user.
/// Used by ignore, unignore, and unassign operations.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record PerformedByRequest
{
    /// <summary>
    /// Gets the identifier of the user performing the action.
    /// </summary>
    public required string PerformedBy { get; init; }
}

/// <summary>
/// Request body for updating the resolution status of an issue.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record UpdateResolutionStatusRequest
{
    /// <summary>
    /// Gets the new resolution status (None, Todo, InProgress, Resolved).
    /// </summary>
    public required string Status { get; init; }

    /// <summary>
    /// Gets the identifier of the user performing the action.
    /// </summary>
    public required string PerformedBy { get; init; }
}

/// <summary>
/// Request body for assigning an issue to a user.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record AssignIssueRequest
{
    /// <summary>
    /// Gets the identifier of the user to assign the issue to.
    /// </summary>
    public required string AssignedTo { get; init; }

    /// <summary>
    /// Gets the identifier of the user performing the action.
    /// </summary>
    public required string PerformedBy { get; init; }
}

/// <summary>
/// Response for issue management commands.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record IssueCommandResponse
{
    /// <summary>
    /// Gets the issue ID.
    /// </summary>
    public required string IssueId { get; init; }

    /// <summary>
    /// Gets the result message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the response.
    /// </summary>
    public DateTime Timestamp { get; init; }
}

/// <summary>
/// Response for paginated issue history entries.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record IssueHistoryResponse
{
    /// <summary>
    /// Gets the issue ID.
    /// </summary>
    public required string IssueId { get; init; }

    /// <summary>
    /// Gets the number of records skipped.
    /// </summary>
    public int Skip { get; init; }

    /// <summary>
    /// Gets the maximum number of records requested.
    /// </summary>
    public int Top { get; init; }

    /// <summary>
    /// Gets the actual number of entries returned.
    /// </summary>
    public int Count { get; init; }

    /// <summary>
    /// Gets the list of history entries.
    /// </summary>
    public required IReadOnlyList<IssueHistoryEntryDto> Entries { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the response.
    /// </summary>
    public DateTime Timestamp { get; init; }
}

#endregion
