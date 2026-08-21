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
    private const int MaxPageSize = 100;

    /// <summary>Recent ETL imports, most recently requested first, so a caller that no longer has an
    /// import id can still find its run.</summary>
    /// <param name="skip">Imports to skip (default 0)</param>
    /// <param name="top">Imports to return (default 10, max 100)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    [HttpGet]
    [ProducesResponseType(typeof(EtlImportListResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetImports(
        [FromQuery] int skip = 0,
        [FromQuery] int top = 10,
        CancellationToken cancellationToken = default)
    {
        if (skip < 0)
        {
            return BadRequest(new ErrorResponse { Message = "Skip must be greater than or equal to 0." });
        }

        if (top <= 0 || top > MaxPageSize)
        {
            return BadRequest(new ErrorResponse { Message = $"Top must be between 1 and {MaxPageSize}." });
        }

        var page = await statusStore.ListAsync(skip, top, cancellationToken);

        return Ok(new EtlImportListResponse
        {
            Skip = skip,
            Top = top,
            Count = page.Imports.Count,
            TotalCount = page.TotalCount,
            Imports = [.. page.Imports.Select(Summarise)]
        });
    }

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

    private static EtlImportSummaryResponse Summarise(EtlImportDocument document) => new()
    {
        ImportId = document.ImportId,
        Status = document.Status,
        SourceType = document.SourceType,
        Dataset = document.Dataset,
        RequestedAtUtc = document.RequestedAtUtc,
        StartedAtUtc = document.StartedAtUtc,
        CompletedAtUtc = document.CompletedAtUtc,
        CurrentStage = document.CurrentStage,
        DatasetCount = document.Datasets.Count,
        SourceFileCount = document.Datasets.Sum(d => d.SourceFiles.Count),
        RowCount = document.Datasets.Any(d => d.RowCount.HasValue)
            ? document.Datasets.Sum(d => d.RowCount ?? 0)
            : null,
        DuckDbPath = Qualify(EtlPipelineFolders.Staging, document.DuckDbKey),
        Error = document.Error
    };

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
            RowsIgnoredDeletes = d.RowsIgnoredDeletes,
            ColumnsNullified = [.. d.ColumnsNullified],
            ColumnsAdded = [.. d.ColumnsAdded]
        })]
    };

    /// <summary>Stages record keys relative to their folder; callers want the whole path, the same
    /// as the staging endpoint reports.</summary>
    private static string? Qualify(string folder, string? key)
        => key is null ? null : $"{folder}/{key}";
}
