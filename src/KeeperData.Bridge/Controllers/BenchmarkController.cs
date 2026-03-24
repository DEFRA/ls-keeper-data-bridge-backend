using KeeperData.Infrastructure.Benchmarking.Models;
using KeeperData.Infrastructure.Benchmarking.Services;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Controllers;

/// <summary>
/// Invokes and manages self-contained MongoDB benchmark runs.
/// Designed to diagnose environment-level performance differences
/// without touching production data collections.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class BenchmarkController(
    IBenchmarkOrchestrator orchestrator,
    ILogger<BenchmarkController> logger) : ControllerBase
{
    private static CancellationTokenSource? s_cts;
    private static readonly object s_lock = new();

    /// <summary>
    /// Starts a benchmark run with the supplied (or default) configuration.
    /// The run executes in the background; poll <c>GET /api/benchmark/report</c> for results.
    /// </summary>
    [HttpPost("start")]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public IActionResult Start([FromBody] BenchmarkConfig? config = null)
    {
        config ??= new BenchmarkConfig();

        if (orchestrator.IsRunning)
        {
            return Conflict(new { message = "A benchmark is already running." });
        }

        CancellationTokenSource cts;
        lock (s_lock)
        {
            s_cts?.Dispose();
            s_cts = new CancellationTokenSource();
            cts = s_cts;
        }

        // Fire-and-forget; the result is retrieved via /report
        _ = Task.Run(async () =>
        {
            try
            {
                logger.LogInformation("Benchmark run started via API");
                await orchestrator.RunAsync(config, cts.Token);
                logger.LogInformation("Benchmark run completed");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Benchmark run failed unexpectedly");
            }
        }, CancellationToken.None);

        return Accepted(new
        {
            message = "Benchmark started. Poll GET /api/benchmark/report for results.",
            config
        });
    }

    /// <summary>
    /// Cancels the currently running benchmark. Collections are cleaned up automatically.
    /// </summary>
    [HttpPost("cancel")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public IActionResult Cancel()
    {
        if (!orchestrator.IsRunning)
        {
            return NotFound(new { message = "No benchmark is currently running." });
        }

        lock (s_lock)
        {
            s_cts?.Cancel();
        }

        logger.LogInformation("Benchmark cancellation requested via API");
        return Ok(new { message = "Benchmark cancellation requested. Collections will be cleaned up." });
    }

    /// <summary>
    /// Returns the last benchmark report, or 404 if no run has completed yet.
    /// </summary>
    [HttpGet("report")]
    [ProducesResponseType(typeof(BenchmarkReport), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public IActionResult GetReport()
    {
        var report = orchestrator.LastReport;
        if (report is null)
        {
            return NotFound(new { message = "No benchmark report available. Start a benchmark first." });
        }

        return Ok(report);
    }

    /// <summary>
    /// Returns the current status of the benchmark subsystem.
    /// </summary>
    [HttpGet("status")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public IActionResult GetStatus()
    {
        return Ok(new
        {
            isRunning = orchestrator.IsRunning,
            hasReport = orchestrator.LastReport is not null,
            lastReportStatus = orchestrator.LastReport?.Status,
            lastReportTimestamp = orchestrator.LastReport?.TimestampUtc
        });
    }
}
