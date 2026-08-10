using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Models;
using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Infrastructure.Storage;
using Microsoft.AspNetCore.Mvc;

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
    ILogger<EtlImportController> logger) : ControllerBase
{
    /// <summary>
    /// Starts an ETL import over whatever is currently in the source folder and returns
    /// immediately with an import id to poll. No file is uploaded here: source files are put in
    /// place beforehand, and the run discovers them.
    /// </summary>
    /// <param name="sourceType">The source type for the import ("internal" or "external")</param>
    /// <param name="dataset">Restricts the run to one dataset, e.g. "sam_cph_holdings". Omit to run all.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    [HttpPost]
    [ProducesResponseType(typeof(StartFileBasedImportResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(FileBasedImportConflictResponse), StatusCodes.Status409Conflict)]
    public async Task<IActionResult> StartImport(
        [FromQuery] string sourceType = BlobStorageSources.External,
        [FromQuery] string? dataset = null,
        CancellationToken cancellationToken = default)
    {
        if (sourceType != BlobStorageSources.Internal && sourceType != BlobStorageSources.External)
        {
            return BadRequest(new ErrorResponse
            {
                Message = $"Invalid sourceType '{sourceType}'. Must be '{BlobStorageSources.Internal}' or '{BlobStorageSources.External}'."
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
            "Received request to start ETL import (sourceType={sourceType}, dataset={dataset})",
            sourceType,
            dataset ?? "all");

        var result = await coordinator.StartAsync(sourceType, dataset, cancellationToken);

        if (!result.Accepted)
        {
            return Conflict(new FileBasedImportConflictResponse
            {
                Message = "An ETL import is already running. Poll that import, or retry when it has finished.",
                InFlightImportId = result.InFlightImportId
            });
        }

        return Accepted(new StartFileBasedImportResponse
        {
            ImportId = result.ImportId!.Value,
            Status = EtlImportStatus.Queued.ToString()
        });
    }

    private bool IsKnownDataset(string dataset)
        => dataSetDefinitions.All.Any(d => string.Equals(d.Name, dataset, StringComparison.OrdinalIgnoreCase));
}
