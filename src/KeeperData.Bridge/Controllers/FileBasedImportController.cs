using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Models;
using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Infrastructure.Storage;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

/// <summary>Trigger and status for the file-based ETL pipeline.
///
/// Separate from <c>api/import</c>, which drives the legacy Mongo import and is unaffected by
/// anything here.</summary>
[ApiController]
[Route("api/etl/file-based/imports")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class FileBasedImportController(
    IFileBasedImportCoordinator coordinator,
    IEtlImportStatusStore statusStore,
    IDataSetDefinitions dataSetDefinitions,
    ILogger<FileBasedImportController> logger) : ControllerBase
{
    /// <summary>
    /// Starts a file-based ETL import over whatever is currently in the source folder and returns
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
            "Received request to start file-based import (sourceType={sourceType}, dataset={dataset})",
            sourceType,
            dataset ?? "all");

        var result = await coordinator.StartAsync(sourceType, dataset, cancellationToken);

        if (!result.Accepted)
        {
            return Conflict(new FileBasedImportConflictResponse
            {
                Message = "A file-based ETL import is already running. Poll that import, or retry when it has finished.",
                InFlightImportId = result.InFlightImportId
            });
        }

        return Accepted(new StartFileBasedImportResponse
        {
            ImportId = result.ImportId!.Value,
            Status = EtlImportStatus.Queued.ToString()
        });
    }

    /// <summary>Current status of a file-based ETL import.</summary>
    [HttpGet("{importId:guid}")]
    [ProducesResponseType(typeof(FileBasedImportStatusResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetImportStatus(Guid importId, CancellationToken cancellationToken = default)
    {
        var document = await statusStore.GetAsync(importId, cancellationToken);

        if (document is null)
        {
            return NotFound(new ErrorResponse { Message = $"No file-based import found with id '{importId}'." });
        }

        return Ok(Map(document));
    }

    private bool IsKnownDataset(string dataset)
        => dataSetDefinitions.All.Any(d => string.Equals(d.Name, dataset, StringComparison.OrdinalIgnoreCase));

    private static FileBasedImportStatusResponse Map(EtlImportDocument document) => new()
    {
        ImportId = document.ImportId,
        Status = document.Status,
        SourceType = document.SourceType,
        Dataset = document.Dataset,
        RequestedAtUtc = document.RequestedAtUtc,
        StartedAtUtc = document.StartedAtUtc,
        CompletedAtUtc = document.CompletedAtUtc,
        CurrentStage = document.CurrentStage,
        DuckDbPath = Qualify(EtlPipelineFolders.Staging, document.DuckDbKey),
        Error = document.Error,
        Stages = [.. document.Stages.Select(s => new FileBasedImportStageResponse
        {
            Name = s.Name,
            ItemCount = s.ItemCount,
            ElapsedMs = s.ElapsedMs,
            CompletedAtUtc = s.CompletedAtUtc
        })],
        Datasets = [.. document.Datasets.Select(d => new FileBasedImportDatasetResponse
        {
            Dataset = d.Dataset,
            SourceFiles = [.. d.SourceFiles.Select(f => new FileBasedImportSourceFileResponse { Key = f.Key, Size = f.Size })],
            RawPaths = [.. d.RawKeys.Select(k => Qualify(EtlPipelineFolders.Raw, k)!)],
            NormalisedPaths = [.. d.NormalisedKeys.Select(k => Qualify(EtlPipelineFolders.Normalised, k)!)],
            SnapshotPath = Qualify(EtlPipelineFolders.Snapshots, d.SnapshotKey),
            SnapshotSourceTimestampUtc = d.SnapshotSourceTimestampUtc,
            RowCount = d.RowCount,
            RowsUpserted = d.RowsUpserted,
            RowsIgnoredDeletes = d.RowsIgnoredDeletes
        })]
    };

    /// <summary>Stages record keys relative to their folder; callers want the whole path, the same
    /// as the staging endpoint reports.</summary>
    private static string? Qualify(string folder, string? key)
        => key is null ? null : $"{folder}/{key}";
}
