using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.SamCtsHoldings.Query.Domain;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/[controller]")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class HoldingsController(
    ICleanseFacade cleanseFacade,
    ILogger<HoldingsController> logger) : ControllerBase
{
    /// <summary>
    /// Gets a CTS CPH holding by its LID full identifier (format: XX-CC/PPP/HHHH).
    /// </summary>
    /// <param name="lidFullIdentifier">The LID full identifier (e.g., "AB-12/345/6789")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The CTS CPH holding details, or 404 if not found</returns>
    [HttpGet("cts/{lidFullIdentifier}")]
    [ProducesResponseType(typeof(CtsCphHoldingResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> GetCtsCphHolding(string lidFullIdentifier, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get CTS CPH holding for lidFullIdentifier={LidFullIdentifier}", lidFullIdentifier);

        var parsed = LidFullIdentifier.TryParse(lidFullIdentifier);
        if (parsed is null)
        {
            logger.LogWarning("Invalid LID full identifier format: {LidFullIdentifier}", lidFullIdentifier);
            return BadRequest(new ErrorResponse
            {
                Message = $"Invalid LID full identifier '{lidFullIdentifier}'. Expected format: XX-CC/PPP/HHHH",
                Timestamp = DateTime.UtcNow
            });
        }

        var holding = await cleanseFacade.Queries.CtsSamQueryService.GetCtsCphHoldingAsync(parsed, cancellationToken);

        if (holding is null)
        {
            logger.LogWarning("CTS CPH holding not found for lidFullIdentifier={LidFullIdentifier}", lidFullIdentifier);
            return NotFound(new ErrorResponse
            {
                Message = $"CTS CPH holding not found for LID full identifier: {lidFullIdentifier}",
                Timestamp = DateTime.UtcNow
            });
        }

        logger.LogInformation("Successfully retrieved CTS CPH holding for lidFullIdentifier={LidFullIdentifier}", lidFullIdentifier);

        return Ok(new CtsCphHoldingResponse
        {
            LidFullIdentifier = holding.Id.Value,
            LocationName = holding.LocationName,
            Holding = holding.Holding,
            Keepers = holding.Keepers,
            Timestamp = DateTime.UtcNow
        });
    }

    /// <summary>
    /// Gets a SAM CPH holding by its CPH value (format: CC/PPP/HHHH).
    /// </summary>
    /// <param name="cph">The CPH value (e.g., "12/345/6789")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The SAM CPH holding details, or 404 if not found</returns>
    [HttpGet("sam/{cph}")]
    [ProducesResponseType(typeof(SamCphHoldingResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    public async Task<IActionResult> GetSamCphHolding(string cph, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request to get SAM CPH holding for cph={Cph}", cph);

        var parsed = Cph.TryParse(cph);
        if (parsed is null)
        {
            logger.LogWarning("Invalid CPH format: {Cph}", cph);
            return BadRequest(new ErrorResponse
            {
                Message = $"Invalid CPH '{cph}'. Expected format: CC/PPP/HHHH",
                Timestamp = DateTime.UtcNow
            });
        }

        var holding = await cleanseFacade.Queries.CtsSamQueryService.GetSamCphHoldingAsync(parsed, cancellationToken);

        if (holding is null)
        {
            logger.LogWarning("SAM CPH holding not found for cph={Cph}", cph);
            return NotFound(new ErrorResponse
            {
                Message = $"SAM CPH holding not found for CPH: {cph}",
                Timestamp = DateTime.UtcNow
            });
        }

        logger.LogInformation("Successfully retrieved SAM CPH holding for cph={Cph}", cph);

        return Ok(new SamCphHoldingResponse
        {
            Cph = holding.Cph.Value,
            LocationName = holding.LocationName,
            Holding = holding.Holding,
            Herd = holding.Herd,
            Parties = holding.Parties,
            Holders = holding.Holders,
            Timestamp = DateTime.UtcNow
        });
    }
}

#region Response DTOs

/// <summary>
/// Response containing CTS CPH holding details.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record CtsCphHoldingResponse
{
    /// <summary>
    /// Gets the LID full identifier.
    /// </summary>
    public required string LidFullIdentifier { get; init; }

    /// <summary>
    /// Gets the location name (ADR_NAME in CTS).
    /// </summary>
    public string? LocationName { get; init; }

    /// <summary>
    /// Gets the holding data.
    /// </summary>
    public required Dictionary<string, object?> Holding { get; init; }

    /// <summary>
    /// Gets the keepers associated with the holding.
    /// </summary>
    public required QueryResult Keepers { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the response.
    /// </summary>
    public DateTime Timestamp { get; init; }
}

/// <summary>
/// Response containing SAM CPH holding details.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record SamCphHoldingResponse
{
    /// <summary>
    /// Gets the CPH value.
    /// </summary>
    public required string Cph { get; init; }

    /// <summary>
    /// Gets the location name (FEATURE_NAME in SAM).
    /// </summary>
    public string? LocationName { get; init; }

    /// <summary>
    /// Gets the holding data.
    /// </summary>
    public required Dictionary<string, object?> Holding { get; init; }

    /// <summary>
    /// Gets the herd data associated with the holding.
    /// </summary>
    public required QueryResult Herd { get; init; }

    /// <summary>
    /// Gets the party data associated with the holding.
    /// </summary>
    public required QueryResult Parties { get; init; }

    /// <summary>
    /// Gets the holder data associated with the holding.
    /// </summary>
    public required QueryResult Holders { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the response.
    /// </summary>
    public DateTime Timestamp { get; init; }
}

#endregion
