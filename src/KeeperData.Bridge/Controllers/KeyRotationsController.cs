using KeeperData.Application;
using KeeperData.Application.Commands.KeyRotations;
using KeeperData.Application.Queries.KeyRotations;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Storage.KeyRotation;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/key-rotations")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class KeyRotationsController(
    IRequestExecutor requestExecutor,
    ILogger<KeyRotationsController> logger) : ControllerBase
{
    private const string LogPrefix = "[KeyRotation]";

    /// <summary>
    /// Gets successful external storage key rotations, most recent first.
    /// Key ids are masked (first three and last three characters); secrets are never returned.
    /// </summary>
    /// <param name="page">1-based page number (default 1)</param>
    /// <param name="pageSize">Page size, 1-100 (default 10)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>A page of key rotation summaries</returns>
    [HttpGet]
    [ProducesResponseType(typeof(KeyRotationListResult), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetKeyRotations(
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 10,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("{LogPrefix} Received request to list key rotations (page={Page}, pageSize={PageSize})",
            LogPrefix, page, pageSize);

        if (page < 1)
        {
            return BadRequest(new ErrorResponse
            {
                Message = "page must be greater than or equal to 1.",
                Timestamp = DateTime.UtcNow
            });
        }

        if (pageSize < 1 || pageSize > GetKeyRotationsQueryValidator.MaxPageSize)
        {
            return BadRequest(new ErrorResponse
            {
                Message = $"pageSize must be between 1 and {GetKeyRotationsQueryValidator.MaxPageSize}.",
                Timestamp = DateTime.UtcNow
            });
        }

        var result = await requestExecutor.ExecuteQuery(new GetKeyRotationsQuery(page, pageSize), cancellationToken);

        return Ok(result);
    }

    /// <summary>
    /// Runs the key rotation check on demand — the same detect/validate/adopt flow the
    /// scheduled daily job performs. Returns the outcome of the check; when a new key file
    /// is found and validates, it is adopted immediately. Progress and outcome are logged
    /// with the [KeyRotation] prefix, and any adopted rotation appears in GET /api/key-rotations.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The outcome of the rotation check</returns>
    [HttpPost("check")]
    [ProducesResponseType(typeof(KeyRotationCheckResponse), StatusCodes.Status200OK)]
    public async Task<IActionResult> RunKeyRotationCheck(CancellationToken cancellationToken = default)
    {
        logger.LogInformation("{LogPrefix} Received request to run an on-demand key rotation check", LogPrefix);

        var result = await requestExecutor.ExecuteCommand(new RunKeyRotationCheckCommand(), cancellationToken);

        return Ok(result);
    }

    /// <summary>
    /// Manually applies a new access key id and secret. The credentials are validated
    /// against the external bucket before being encrypted, stored, and activated.
    /// The submitted secret is never echoed back.
    /// </summary>
    /// <param name="request">The credentials to apply</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The created rotation summary (masked)</returns>
    [HttpPost]
    [ProducesResponseType(typeof(KeyRotationListItem), StatusCodes.Status201Created)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status409Conflict)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status503ServiceUnavailable)]
    public async Task<IActionResult> ApplyManualKey(
        [FromBody] ApplyManualKeyRequest request,
        CancellationToken cancellationToken = default)
    {
        // Deliberately do not log the request body: it contains live credentials.
        logger.LogInformation("{LogPrefix} Received request to manually apply a new access key", LogPrefix);

        if (string.IsNullOrWhiteSpace(request.AccessKeyId) || string.IsNullOrWhiteSpace(request.SecretAccessKey))
        {
            return BadRequest(new ErrorResponse
            {
                Message = "Both accessKeyId and secretAccessKey are required.",
                Timestamp = DateTime.UtcNow
            });
        }

        var result = await requestExecutor.ExecuteCommand(
            new ApplyManualKeyCommand(request.AccessKeyId, request.SecretAccessKey), cancellationToken);

        return result.Outcome switch
        {
            KeyRotationActionOutcome.Applied => Created($"/api/key-rotations", result.Rotation),
            KeyRotationActionOutcome.ValidationFailed or KeyRotationActionOutcome.InvalidRequest =>
                BadRequest(Error(result.Detail ?? "The supplied credentials failed validation against the bucket.")),
            KeyRotationActionOutcome.NotConfigured =>
                Conflict(Error(result.Detail ?? "Key rotation is not configured.")),
            _ => StatusCode(StatusCodes.Status503ServiceUnavailable,
                Error(result.Detail ?? "The rotation could not be completed right now; please retry."))
        };
    }

    /// <summary>
    /// Rolls back to the credentials captured in a previous rotation. The old credentials
    /// are re-validated against the bucket before being re-activated as a new rotation record.
    /// </summary>
    /// <param name="id">The rotation record id to roll back to</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The created rollback rotation summary (masked)</returns>
    [HttpPost("{id}/rollback")]
    [ProducesResponseType(typeof(KeyRotationListItem), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status409Conflict)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status503ServiceUnavailable)]
    public async Task<IActionResult> RollbackKeyRotation(
        string id,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("{LogPrefix} Received request to roll back to rotation {RotationId}", LogPrefix, id);

        var result = await requestExecutor.ExecuteCommand(new RollbackKeyRotationCommand(id), cancellationToken);

        return result.Outcome switch
        {
            KeyRotationActionOutcome.Applied => Ok(result.Rotation),
            KeyRotationActionOutcome.NotFound =>
                NotFound(Error(result.Detail ?? $"No rotation record found with id '{id}'.")),
            KeyRotationActionOutcome.ValidationFailed =>
                Conflict(Error(result.Detail ?? "The rotation's credentials no longer authenticate against the bucket.")),
            KeyRotationActionOutcome.InvalidRequest or KeyRotationActionOutcome.NotConfigured =>
                Conflict(Error(result.Detail ?? "The rotation cannot be rolled back.")),
            _ => StatusCode(StatusCodes.Status503ServiceUnavailable,
                Error(result.Detail ?? "The rollback could not be completed right now; please retry."))
        };
    }

    private static ErrorResponse Error(string message) => new()
    {
        Message = message,
        Timestamp = DateTime.UtcNow
    };
}

/// <summary>
/// Request body for manually applying external storage credentials. Write-only:
/// the values are validated, encrypted, and stored — never echoed or logged.
/// </summary>
public record ApplyManualKeyRequest
{
    public required string AccessKeyId { get; init; }
    public required string SecretAccessKey { get; init; }
}
