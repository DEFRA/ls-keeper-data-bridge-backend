using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Models;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.EtlPipeline.Storage;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

/// <summary>Polls the status of an ETL pipeline run.
///
/// Separate from <c>api/import</c>, which drives the legacy Mongo import and is unaffected by
/// anything here.</summary>
[ApiController]
[Route("api/etl/imports")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class EtlImportStatusController(
    IEtlImportStatusStore statusStore) : ControllerBase
{
    /// <summary>Current status of an ETL import.</summary>
    [HttpGet("{importId:guid}")]
    [ProducesResponseType(typeof(EtlImportStatusResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetImportStatus(Guid importId, CancellationToken cancellationToken = default)
    {
        var document = await statusStore.GetAsync(importId, cancellationToken);

        if (document is null)
        {
            return NotFound(new ErrorResponse { Message = $"No ETL import found with id '{importId}'." });
        }

        return Ok(Map(document));
    }

    private static EtlImportStatusResponse Map(EtlImportDocument document) => new()
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
        Stages = [.. document.Stages.Select(s => new EtlImportStageResponse
        {
            Name = s.Name,
            ItemCount = s.ItemCount,
            ElapsedMs = s.ElapsedMs,
            CompletedAtUtc = s.CompletedAtUtc
        })],
        Datasets = [.. document.Datasets.Select(d => new EtlImportDatasetResponse
        {
            Dataset = d.Dataset,
            SourceFiles = [.. d.SourceFiles.Select(f => new EtlImportSourceFileResponse { Key = f.Key, Size = f.Size })],
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
