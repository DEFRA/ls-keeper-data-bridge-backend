using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Config;
using KeeperData.Bridge.Models;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Storage;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Microsoft.Extensions.Options;

namespace KeeperData.Bridge.Controllers;

/// <summary>Test-support operations for ETL stage storage.</summary>
[ApiController]
[Route("api/etl/storage")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component tests.")]
public sealed class EtlStorageController(
    IBlobStorageServiceFactory blobStorageServiceFactory,
    IEtlPipelineStorageProvider storageProvider,
    IDataSetDefinitions dataSetDefinitions,
    IWebHostEnvironment environment,
    IOptions<FeatureFlags> featureFlags,
    TimeProvider timeProvider,
    ILogger<EtlStorageController> logger) : ControllerBase
{
    private const string All = "all";
    private const string Inbound = "inbound";
    private const string Raw = "raw";
    private const string Normalised = "normalised";
    private const string Snapshots = "snapshots";
    private const string Staging = "staging";

    private static readonly string[] DatasetStages = [Inbound, Raw, Normalised, Snapshots];
    private static readonly string[] EveryStage = [.. DatasetStages, Staging];

    /// <summary>
    /// Purges ETL stage data in non-production environments, or when explicitly enabled for a
    /// Production-hosted ephemeral deployment. A dataset-scoped purge deliberately excludes staging
    /// because staging databases contain every dataset. Dataset and stage must be supplied explicitly;
    /// use dataset=all and stage=all to request a complete purge.
    /// </summary>
    [HttpDelete]
    [ProducesResponseType(typeof(EtlStoragePurgeResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status403Forbidden)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> PurgeStorage(
        [FromQuery, BindRequired] string? dataset,
        [FromQuery, BindRequired] string? stage,
        [FromQuery] string? sourceType = BlobStorageSources.Internal,
        CancellationToken cancellationToken = default)
    {
        if (environment.IsProduction() && !featureFlags.Value.EtlStoragePurgeEnabled)
        {
            logger.LogWarning(
                "Rejected an ETL storage purge request in Production because it was not explicitly enabled");
            return StatusCode(StatusCodes.Status403Forbidden, Error(
                "Storage purge endpoint is disabled in production environments."));
        }

        if (string.IsNullOrWhiteSpace(dataset) || string.IsNullOrWhiteSpace(stage))
        {
            return BadRequest(Error(
                "The dataset and stage query parameters are required. " +
                "Use the explicit value 'all' to request an all-dataset or all-stage purge."));
        }

        var requestedStage = Normalise(stage, All);
        if (!EveryStage.Contains(requestedStage, StringComparer.Ordinal) && requestedStage != All)
        {
            return BadRequest(Error(
                $"Invalid stage '{stage}'. Allowed values: all, inbound, raw, normalised, snapshots, staging."));
        }

        var requestedSourceType = Normalise(sourceType, BlobStorageSources.Internal);
        if (requestedSourceType != BlobStorageSources.Internal
            && requestedSourceType != BlobStorageSources.External)
        {
            return BadRequest(Error(
                $"Invalid sourceType '{sourceType}'. Must be '{BlobStorageSources.Internal}' or '{BlobStorageSources.External}'."));
        }

        var requestedDataset = Normalise(dataset, All);
        var definition = requestedDataset == All
            ? null
            : dataSetDefinitions.All.FirstOrDefault(candidate =>
                string.Equals(candidate.Name, requestedDataset, StringComparison.OrdinalIgnoreCase));

        if (requestedDataset != All && definition is null)
        {
            return BadRequest(Error($"Dataset '{dataset}' is not recognized."));
        }

        if (definition is not null && requestedStage == Staging)
        {
            return BadRequest(Error(
                "The staging folder contains a shared all-dataset database and cannot be purged by dataset. " +
                "Use dataset=all with stage=staging."));
        }

        var stages = ResolveStages(requestedStage, definition);

        var deletedKeys = new List<string>();

        try
        {
            foreach (var targetStage in stages)
            {
                var target = ResolveTarget(targetStage, requestedSourceType, definition);
                deletedKeys.AddRange(await DeleteTargetAsync(target, cancellationToken));
            }

            var purgedAtUtc = timeProvider.GetUtcNow().UtcDateTime;
            logger.LogInformation(
                "Purged {DeletedCount} ETL storage object(s) for dataset {Dataset}, stage {Stage}, source type {SourceType}",
                deletedKeys.Count,
                definition?.Name ?? All,
                requestedStage,
                requestedSourceType);

            return Ok(new EtlStoragePurgeResponse
            {
                Success = true,
                DeletedCount = deletedKeys.Count,
                DeletedKeys = deletedKeys,
                Message = $"Successfully purged {deletedKeys.Count} object(s) from S3 stage storage.",
                PurgedAtUtc = purgedAtUtc
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("ETL storage purge request was cancelled");
            return StatusCode(StatusCodes.Status499ClientClosedRequest, Error("Request was cancelled."));
        }
        catch (Exception exception)
        {
            logger.LogError(
                exception,
                "Failed to purge ETL storage for dataset {Dataset}, stage {Stage}, source type {SourceType}",
                definition?.Name ?? All,
                requestedStage,
                requestedSourceType);

            return StatusCode(StatusCodes.Status500InternalServerError,
                Error("Failed to purge S3 stage storage."));
        }
    }

    private PurgeTarget ResolveTarget(
        string stage,
        string sourceType,
        DataSetDefinition? definition)
        => stage switch
        {
            Inbound => new PurgeTarget(
                sourceType == BlobStorageSources.External
                    ? blobStorageServiceFactory.GetSourceInternal()
                    : blobStorageServiceFactory.Get(),
                definition is null ? null : DataSetFileNaming.DataSetKeyPrefix(definition),
                sourceType == BlobStorageSources.External ? "qasrc" : "dest"),
            Raw => PipelineTarget(EtlPipelineFolders.Raw, definition is null
                ? null
                : DataSetFileNaming.DataSetKeyPrefix(definition)),
            Normalised => PipelineTarget(EtlPipelineFolders.Normalised, definition is null
                ? null
                : SnapshotFileNaming.DataSetPrefix(definition)),
            Snapshots => PipelineTarget(EtlPipelineFolders.Snapshots, definition is null
                ? null
                : SnapshotFileNaming.DataSetPrefix(definition)),
            Staging => PipelineTarget(EtlPipelineFolders.Staging, null),
            _ => throw new InvalidOperationException($"Unsupported ETL storage stage '{stage}'.")
        };

    private static string[] ResolveStages(string requestedStage, DataSetDefinition? definition)
    {
        if (requestedStage != All)
        {
            return [requestedStage];
        }

        // A targeted all-stage purge must not remove the shared all-dataset DuckDB artifacts.
        return definition is null ? EveryStage : DatasetStages;
    }

    private PurgeTarget PipelineTarget(string folder, string? prefix)
        => new(storageProvider.ForFolder(folder), prefix, folder);

    private static async Task<IReadOnlyList<string>> DeleteTargetAsync(
        PurgeTarget target,
        CancellationToken cancellationToken)
    {
        var result = await target.Storage.DeleteByPrefixAsync(
            target.Prefix ?? string.Empty,
            cancellationToken);

        return result.DeletedKeys
            .Select(key => $"{target.DisplayFolder}/{key.TrimStart('/')}")
            .ToArray();
    }

    private ErrorResponse Error(string message)
        => new()
        {
            Message = message,
            Timestamp = timeProvider.GetUtcNow().UtcDateTime
        };

    private static string Normalise(string? value, string defaultValue)
        => string.IsNullOrWhiteSpace(value) ? defaultValue : value.Trim().ToLowerInvariant();

    private sealed record PurgeTarget(IBlobStorageService Storage, string? Prefix, string DisplayFolder);
}
