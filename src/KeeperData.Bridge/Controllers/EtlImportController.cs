using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Config;
using KeeperData.Bridge.Models;
using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Infrastructure.Storage;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace KeeperData.Bridge.Controllers;

/// <summary>Triggers an ETL pipeline run.
///
/// Separate from <c>api/import</c>, which drives the legacy Mongo import and is unaffected by
/// anything here.</summary>
[ApiController]
[Route("api/etl/imports")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class EtlImportController(
    IEtlImportCoordinator coordinator,
    IDataSetDefinitions dataSetDefinitions,
    IWebHostEnvironment environment,
    IOptions<FeatureFlags> featureFlags,
    ILogger<EtlImportController> logger) : ControllerBase
{
    /// <summary>
    /// Starts an ETL import over whatever is currently in the source folder and returns
    /// immediately with an import id to poll. No file is uploaded here: source files are put in
    /// place beforehand, and the run discovers them.
    /// </summary>
    /// <param name="sourceType">The source type for the import ("internal" or "external")</param>
    /// <param name="dataset">Restricts the run to one dataset, e.g. "sam_cph_holdings". Omit to run all.</param>
    /// <param name="rebuild">Clears every stage first, so the run rebuilds from the source files.
    /// Governed by the same flag as the purge endpoint, because it deletes the same artefacts.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    [HttpPost]
    [ProducesResponseType(typeof(StartEtlImportResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status403Forbidden)]
    [ProducesResponseType(typeof(EtlImportConflictResponse), StatusCodes.Status409Conflict)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> StartImport(
        [FromQuery] string sourceType = BlobStorageSources.External,
        [FromQuery] string? dataset = null,
        [FromQuery] bool rebuild = false,
        CancellationToken cancellationToken = default)
    {
        if (sourceType != BlobStorageSources.Internal && sourceType != BlobStorageSources.External)
        {
            return BadRequest(new ErrorResponse
            {
                Message = $"Invalid sourceType '{sourceType}'. Must be '{BlobStorageSources.Internal}' or '{BlobStorageSources.External}'."
            });
        }

        // A rebuild deletes exactly what the purge endpoint deletes, so it answers to the same
        // control: allowing it here would be a way around a deliberate production safeguard.
        if (rebuild && environment.IsProduction() && !featureFlags.Value.EtlStoragePurgeEnabled)
        {
            logger.LogWarning("Rejected an ETL rebuild request in Production because it was not explicitly enabled");

            return StatusCode(StatusCodes.Status403Forbidden, new ErrorResponse
            {
                Message = "Rebuild is disabled in production environments."
            });
        }

        if (dataset is not null && !IsKnownDataset(dataset))
        {
            // Rejected here rather than after acceptance: an unknown name would otherwise produce a
            // successful run that silently did nothing.
            return BadRequest(new ErrorResponse
            {
                Message = $"Unknown dataset '{dataset}'."
            });
        }

        logger.LogInformation(
            "Received request to start ETL import (sourceType={sourceType}, dataset={dataset}, rebuild={rebuild})",
            sourceType,
            dataset ?? "all",
            rebuild);

        var result = await coordinator.StartAsync(sourceType, dataset, rebuild, cancellationToken);

        if (result.RebuildError is not null)
        {
            return StatusCode(StatusCodes.Status500InternalServerError, new ErrorResponse
            {
                Message = result.ClearedStages is { Count: > 0 } clearedStages
                    ? $"{result.RebuildError} No import was started, but {string.Join(", ", clearedStages)} " +
                      "had already been cleared, so the staging area is now incomplete."
                    : $"{result.RebuildError} Nothing has been cleared and no import was started."
            });
        }

        if (!result.Accepted)
        {
            return Conflict(new EtlImportConflictResponse
            {
                Message = "An ETL import is already running. Poll that import, or retry when it has finished.",
                InFlightImportId = result.InFlightImportId
            });
        }

        return Accepted(new StartEtlImportResponse
        {
            ImportId = result.ImportId!.Value,
            Status = EtlImportStatus.Queued.ToString()
        });
    }

    private bool IsKnownDataset(string dataset)
        => dataSetDefinitions.All.Any(d => string.Equals(d.Name, dataset, StringComparison.OrdinalIgnoreCase));
}
