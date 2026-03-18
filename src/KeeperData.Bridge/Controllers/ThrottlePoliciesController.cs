using KeeperData.Core.Exceptions;
using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Commands;
using KeeperData.Core.Throttling.Models;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/throttle-policies")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class ThrottlePoliciesController(
    IThrottlePolicyQueryService queryService,
    IThrottlePolicyCommandService commandService,
    ILogger<ThrottlePoliciesController> logger) : ControllerBase
{
    [HttpGet]
    [ProducesResponseType<IReadOnlyList<ThrottlePolicy>>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetAll(CancellationToken ct)
    {
        var policies = await queryService.GetAllAsync(ct);
        return Ok(policies);
    }

    [HttpGet("active")]
    [ProducesResponseType<ThrottlePolicy>(StatusCodes.Status200OK)]
    public IActionResult GetActive()
    {
        var policy = queryService.GetActive();
        return Ok(policy);
    }

    [HttpGet("{slug}")]
    [ProducesResponseType<ThrottlePolicy>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetBySlug(string slug, CancellationToken ct)
    {
        var policy = await queryService.GetBySlugAsync(slug, ct);
        return policy is not null ? Ok(policy) : NotFound();
    }

    [HttpPost]
    [ProducesResponseType<ThrottlePolicy>(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> Create([FromBody] CreateThrottlePolicyCommand command, CancellationToken ct)
    {
        try
        {
            var policy = await commandService.CreateAsync(command, ct);
            logger.LogInformation("Created throttle policy '{Name}' (slug: {Slug})", policy.Name, policy.Slug);
            return CreatedAtAction(nameof(GetBySlug), new { slug = policy.Slug }, policy);
        }
        catch (DomainException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPut("{slug}")]
    [ProducesResponseType<ThrottlePolicy>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Update(string slug, [FromBody] UpdateThrottlePolicyCommand command, CancellationToken ct)
    {
        try
        {
            var policy = await commandService.UpdateAsync(slug, command, ct);
            logger.LogInformation("Updated throttle policy '{Slug}'", slug);
            return Ok(policy);
        }
        catch (NotFoundException)
        {
            return NotFound();
        }
        catch (DomainException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpDelete("{slug}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> Delete(string slug, CancellationToken ct)
    {
        try
        {
            await commandService.DeleteAsync(slug, ct);
            logger.LogInformation("Deleted throttle policy '{Slug}'", slug);
            return NoContent();
        }
        catch (NotFoundException)
        {
            return NotFound();
        }
        catch (DomainException ex) when (ex.Message.Contains("active", StringComparison.OrdinalIgnoreCase))
        {
            return Conflict(new { error = ex.Message });
        }
        catch (DomainException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("{slug}/activate")]
    [ProducesResponseType<ThrottlePolicy>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Activate(string slug, CancellationToken ct)
    {
        try
        {
            var policy = await commandService.ActivateAsync(slug, ct);
            logger.LogInformation("Activated throttle policy '{Slug}'", slug);
            return Ok(policy);
        }
        catch (NotFoundException)
        {
            return NotFound();
        }
        catch (DomainException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("deactivate")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public async Task<IActionResult> DeactivateAll(CancellationToken ct)
    {
        await commandService.DeactivateAllAsync(ct);
        logger.LogInformation("Deactivated all throttle policies; Normal fallback now active");
        return NoContent();
    }
}
