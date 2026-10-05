using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using KeeperData.Bridge.Config;
using KeeperData.Bridge.Models;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
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
    IEtlImportStatusStore statusStore,
    IWebHostEnvironment environment,
    IOptions<FeatureFlags> featureFlags,
    TimeProvider timeProvider,
    ILogger<EtlStorageController> logger) : ControllerBase
{
    private const string All = "all";
    private const string Inbound = "inbound";
    private const string Raw = "raw";
    private const string Normalised = "normalised";
    private const string Optimised = "optimised";
    private const string Snapshots = "snapshots";
    private const string Staging = "staging";
    private const string Views = "views";

    /// <summary>The pipeline order, upstream first. A named stage purges itself and everything after
    /// it: downstream artefacts are derived from it, and are timestamp-keyed and reused rather than
    /// rebuilt, so leaving them behind would keep the old data alive under the same keys.</summary>
    private static readonly string[] StageOrder = [Inbound, Raw, Normalised, Optimised, Snapshots, Staging, Views];

    private const int MaxPageSize = 1000;

    /// <summary>
    /// Lists the objects in a stage folder - key, size and last-modified - for debugging storage
    /// growth and verifying what a stage actually holds. Read-only, so unlike the purge it needs no
    /// feature flag; it also reports on staging and views, which a purge can only touch for every
    /// dataset at once. Totals and dataset groups cover the whole listing; the objects themselves
    /// are returned a page at a time because a folder can hold thousands.
    /// </summary>
    [HttpGet("objects")]
    [ProducesResponseType(typeof(EtlStorageReportResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> ListObjects(
        [FromQuery, BindRequired] string? stage,
        [FromQuery] string? dataset,
        [FromQuery] string? sourceType = BlobStorageSources.Internal,
        [FromQuery] int skip = 0,
        [FromQuery] int top = 500,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(stage))
        {
            return BadRequest(Error(
                "The stage query parameter is required. " +
                "Use the explicit value 'all' to report on every folder."));
        }

        var (stageValid, requestedStage, stageError) = NormalizeAndValidateStage(stage);
        if (!stageValid) return stageError!;

        var (sourceValid, requestedSourceType, sourceError) = NormalizeAndValidateSourceType(sourceType);
        if (!sourceValid) return sourceError!;

        var requestedDataset = Normalise(dataset, All);
        var (definition, defError) = ResolveDefinition(requestedDataset, dataset ?? All);
        if (defError is not null) return defError;

        if (definition is not null && requestedStage is Staging or Views)
        {
            return BadRequest(Error(
                $"The {requestedStage} folder holds artefacts shared across every dataset and cannot be reported by dataset. " +
                $"Use dataset=all with stage={requestedStage}."));
        }

        if (skip < 0)
        {
            return BadRequest(Error("Skip must be greater than or equal to 0."));
        }

        if (top <= 0 || top > MaxPageSize)
        {
            return BadRequest(Error($"Top must be between 1 and {MaxPageSize}."));
        }

        try
        {
            var objects = await GetObjectsForReportAsync(requestedStage, requestedSourceType, definition, cancellationToken);
            var response = BuildReportResponse(objects, requestedStage, requestedSourceType, definition, skip, top);
            return Ok(response);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("ETL storage report request was cancelled");
            return StatusCode(StatusCodes.Status499ClientClosedRequest, Error("Request was cancelled."));
        }
        catch (Exception exception)
        {
            logger.LogError(
                exception,
                "Failed to report on ETL storage for dataset {Dataset}, stage {Stage}, source type {SourceType}",
                definition?.Name ?? All,
                requestedStage,
                requestedSourceType);

            return StatusCode(StatusCodes.Status500InternalServerError,
                Error("Failed to list S3 stage storage."));
        }
    }

    private async Task<List<EtlStorageReportObject>> GetObjectsForReportAsync(string requestedStage, string requestedSourceType, DataSetDefinition? definition, CancellationToken cancellationToken)
    {
        var objects = new List<EtlStorageReportObject>();

        foreach (var reportStage in ReportStages(requestedStage))
        {
            var target = ResolveTarget(reportStage, requestedSourceType, definition);
            await foreach (var item in EnumerateObjectsAsync(target, cancellationToken))
            {
                objects.Add(new EtlStorageReportObject
                {
                    Stage = target.DisplayFolder,
                    Key = item.Key,
                    SizeBytes = item.Size,
                    LastModifiedUtc = item.LastModified
                });
            }
        }

        objects.Sort((a, b) => string.CompareOrdinal(a.Stage, b.Stage)
            is var stageOrder && stageOrder != 0 ? stageOrder : string.CompareOrdinal(a.Key, b.Key));

        return objects;
    }

    private EtlStorageReportResponse BuildReportResponse(List<EtlStorageReportObject> objects, string requestedStage, string requestedSourceType, DataSetDefinition? definition, int skip, int top)
    {
        var byDatasetName = dataSetDefinitions.All
            .ToDictionary(d => d.Name, StringComparer.OrdinalIgnoreCase);

        var groups = objects
            .GroupBy(o => GroupFor(byDatasetName, o.Stage, o.Key))
            .OrderBy(g => g.Key, StringComparer.Ordinal)
            .Select(g => new EtlStorageReportGroup
            {
                Dataset = g.Key,
                ObjectCount = g.Count(),
                SizeBytes = g.Sum(o => o.SizeBytes)
            })
            .ToList();

        return new EtlStorageReportResponse
        {
            Stage = requestedStage,
            Dataset = definition?.Name ?? All,
            SourceType = requestedSourceType,
            ObjectCount = objects.Count,
            TotalSizeBytes = objects.Sum(o => o.SizeBytes),
            Groups = groups,
            Objects = objects.Skip(skip).Take(top).ToList(),
            Skip = skip,
            Top = top
        };
    }

    /// <summary>The stage or stages the report covers. Unlike a purge there is no cascade: a named
    /// stage reports on just its folder, and 'all' reports on every folder in turn.</summary>
    private static string[] ReportStages(string requestedStage)
        => requestedStage == All ? StageOrder : [requestedStage];

    /// <summary>Every object a target's scope covers: the whole prefix, or - where the prefixes alone
    /// are broader than the request, as they are for glob lanes - only the keys that belong to it.</summary>
    private static async IAsyncEnumerable<StorageObjectInfo> EnumerateObjectsAsync(
        StorageTarget target,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var prefix in target.Scope.Prefixes)
        {
            await foreach (var item in target.Storage.EnumerateAsync(prefix, cancellationToken))
            {
                if (target.Scope.Matches?.Invoke(item.Key) == false) continue;

                yield return item;
            }
        }
    }

    /// <summary>The dataset a key belongs to, for the grouped totals. Pipeline lanes keep a dataset's
    /// files under its name; source lanes can be globbed (CTS tables share a folder), so they are
    /// matched the way discovery matches them; the staging and views artefacts belong to every
    /// dataset at once and anything left over is unattributed.</summary>
    private string GroupFor(
        System.Collections.Generic.Dictionary<string, KeeperData.Core.ETL.Impl.DataSetDefinition> byDatasetName,
        string stage,
        string key)
    {
        var separator = key.IndexOf('/');
        var firstSegment = separator >= 0 ? key[..separator] : key;

        if (byDatasetName.TryGetValue(firstSegment, out var byName)) return byName.Name;

        var byPattern = dataSetDefinitions.All.FirstOrDefault(candidate =>
            DataSetFileNaming.Matches(candidate, key));
        if (byPattern is not null) return byPattern.Name;

        return stage is Staging or Views ? "shared" : "other";
    }


    /// <summary>
    /// Purges ETL stage data in non-production environments, or when explicitly enabled for a
    /// Production-hosted ephemeral deployment. Purging a stage also purges everything downstream of
    /// it; staging and views are shared all-dataset artefacts, so a dataset-scoped purge that reaches
    /// them clears them whole. Refused while an import is in flight. Dataset and stage must be
    /// supplied explicitly; use dataset=all and stage=all to request a complete purge.
    /// </summary>
    [HttpDelete]
    [ProducesResponseType(typeof(EtlStoragePurgeResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status403Forbidden)]
    [ProducesResponseType(typeof(EtlImportConflictResponse), StatusCodes.Status409Conflict)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status499ClientClosedRequest)]
    [ProducesResponseType(typeof(ErrorResponse), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> PurgeStorage(
        [FromQuery, BindRequired] string? dataset,
        [FromQuery, BindRequired] string? stage,
        [FromQuery] string? sourceType = BlobStorageSources.Internal,
        CancellationToken cancellationToken = default)
    {
        if (IsStoragePurgeDisabled())
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

        // Validate and normalise stage and sourceType using small helpers to reduce cognitive complexity.
        var (stageValid, requestedStage, stageError) = NormalizeAndValidateStage(stage);
        if (!stageValid) return stageError!;

        var (sourceValid, requestedSourceType, sourceError) = NormalizeAndValidateSourceType(sourceType);
        if (!sourceValid) return sourceError!;

        var requestedDataset = Normalise(dataset, All);
        var (definition, defError) = ResolveDefinition(requestedDataset, dataset!);
        if (defError is not null) return defError;

        if (definition is not null && requestedStage is Staging or Views)
        {
            return BadRequest(Error(
                $"The {requestedStage} folder holds artefacts shared across every dataset and cannot be purged by dataset. " +
                $"Use dataset=all with stage={requestedStage}."));
        }

        var inFlight = await statusStore.GetInFlightAsync(cancellationToken);
        if (inFlight is not null)
        {
            logger.LogWarning(
                "Rejected an ETL storage purge because import {ImportId} is in flight", inFlight.ImportId);
            return Conflict(new EtlImportConflictResponse
            {
                Message = "An ETL import is running. Purging stage storage mid-run would interleave deletes with stage writes; retry when it has finished.",
                InFlightImportId = inFlight.ImportId
            });
        }

        var stages = ResolveStages(requestedStage);

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

            var purgeId = Guid.NewGuid();
            await RecordPurgeAsync(purgeId, requestedSourceType, definition, stages, deletedKeys.Count, cancellationToken);

            return Ok(new EtlStoragePurgeResponse
            {
                Success = true,
                PurgeId = purgeId,
                PurgedStages = stages,
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

    private (bool Valid, string Normalized, IActionResult? Error) NormalizeAndValidateStage(string? stage)
    {
        var requestedStage = Normalise(stage, All);
        if (!StageOrder.Contains(requestedStage, StringComparer.Ordinal) && requestedStage != All)
        {
            return (false, requestedStage, BadRequest(Error(
                $"Invalid stage '{stage}'. Allowed values: all, inbound, raw, normalised, optimised, snapshots, staging, views.")));
        }

        return (true, requestedStage, null);
    }

    private (bool Valid, string Normalized, IActionResult? Error) NormalizeAndValidateSourceType(string? sourceType)
    {
        var requestedSourceType = Normalise(sourceType, BlobStorageSources.Internal);
        if (requestedSourceType != BlobStorageSources.Internal && requestedSourceType != BlobStorageSources.External)
        {
            return (false, requestedSourceType, BadRequest(Error(
                $"Invalid sourceType '{sourceType}'. Must be '{BlobStorageSources.Internal}' or '{BlobStorageSources.External}'.")));
        }

        return (true, requestedSourceType, null);
    }

    private (DataSetDefinition? Definition, IActionResult? Error) ResolveDefinition(string requestedDataset, string originalDataset)
    {
        if (requestedDataset == All) return (null, null);

        var def = dataSetDefinitions.All.FirstOrDefault(candidate =>
            string.Equals(candidate.Name, requestedDataset, StringComparison.OrdinalIgnoreCase));

        if (def is null) return (null, BadRequest(Error($"Dataset '{originalDataset}' is not recognized.")));

        return (def, null);
    }

    private StorageTarget ResolveTarget(
        string stage,
        string sourceType,
        DataSetDefinition? definition)
        => stage switch
        {
            Inbound => new StorageTarget(
                sourceType == BlobStorageSources.External
                    ? blobStorageServiceFactory.GetSourceInternal()
                    : blobStorageServiceFactory.Get(),
                SourceScope(definition),
                sourceType == BlobStorageSources.External ? "qasrc" : "dest"),
            Raw => PipelineTarget(EtlPipelineFolders.Raw, SourceScope(definition)),
            Normalised => PipelineTarget(EtlPipelineFolders.Normalised, PrefixScope(definition is null
                ? null
                : SnapshotFileNaming.DataSetPrefix(definition))),
            Optimised => PipelineTarget(EtlPipelineFolders.Optimised, PrefixScope(definition is null
                ? null
                : SnapshotFileNaming.DataSetPrefix(definition))),
            Snapshots => PipelineTarget(EtlPipelineFolders.Snapshots, PrefixScope(definition is null
                ? null
                : SnapshotFileNaming.DataSetPrefix(definition))),
            Staging => PipelineTarget(EtlPipelineFolders.Staging, PrefixScope(null)),
            Views => PipelineTarget(EtlPipelineFolders.Views, PrefixScope(null)),
            _ => throw new InvalidOperationException($"Unsupported ETL storage stage '{stage}'.")
        };

    /// <summary>The requested stage and every stage downstream of it. Downstream artefacts are
    /// derived: a snapshot rebuilt after its predecessors are purged lands on the same timestamped
    /// key, so staging and views must go too or the next run reuses what they already hold.</summary>
    private static string[] ResolveStages(string requestedStage)
        => requestedStage == All ? StageOrder : StageOrder[Array.IndexOf(StageOrder, requestedStage)..];

    private StorageTarget PipelineTarget(string folder, StorageScope scope)
        => new(storageProvider.ForFolder(folder), scope, folder);

    /// <summary>The keys a stage holding source-named files keeps for one dataset. A dataset naming its
    /// files by a pattern rather than a fixed prefix has no single prefix to work under - the pattern
    /// is not a prefix, and the lane it starts with also holds its sibling datasets - so those lanes are
    /// listed and their keys matched by name instead.</summary>
    private static StorageScope SourceScope(DataSetDefinition? definition)
    {
        if (definition is null) return PrefixScope(null);

        if (definition.SourceKeyPattern is null)
        {
            var prefix = DataSetFileNaming.DataSetKeyPrefix(definition);
            return PrefixScope(prefix);
        }

        var prefixes = DataSetFileNaming.ListingPrefixes(definition);
        bool matches(string key) => DataSetFileNaming.Matches(definition, key);

        return new StorageScope(prefixes, matches);
    }

    private static StorageScope PrefixScope(string? prefix)
        => new([prefix ?? string.Empty], null);

    private static async Task<IReadOnlyList<string>> DeleteTargetAsync(
        StorageTarget target,
        CancellationToken cancellationToken)
    {
        var deleted = new List<string>();

        foreach (var prefix in target.Scope.Prefixes)
        {
            deleted.AddRange(target.Scope.Matches is null
                ? (await target.Storage.DeleteByPrefixAsync(prefix, cancellationToken)).DeletedKeys
                : await DeleteMatchingAsync(target.Storage, prefix, target.Scope.Matches, cancellationToken));
        }

        return [.. deleted.Select(key => $"{target.DisplayFolder}/{key.TrimStart('/')}")];
    }

    /// <summary>A lane can hold more objects than one listing page returns, so it is streamed rather than
    /// listed: a lane whose keys mostly belong to sibling datasets would otherwise be purged as far as the
    /// first page and no further.</summary>
    private static async Task<IReadOnlyList<string>> DeleteMatchingAsync(
        IBlobStorageService storage,
        string prefix,
        Func<string, bool> matches,
        CancellationToken cancellationToken)
    {
        var deleted = new List<string>();

        await foreach (var key in EnumerateKeysAsync(storage, prefix, cancellationToken))
        {
            if (!matches(key)) continue;

            await storage.DeleteAsync(key, cancellationToken);
            deleted.Add(key);
        }

        return deleted;
    }

    /// <summary>The delete already happened, so a history write that fails must not turn the
    /// response into a failure - the purge would look like it never ran while its objects are gone.</summary>
    private async Task RecordPurgeAsync(
        Guid purgeId,
        string sourceType,
        DataSetDefinition? definition,
        IReadOnlyList<string> stages,
        int deletedCount,
        CancellationToken cancellationToken)
    {
        try
        {
            await statusStore.RecordPurgeAsync(
                new EtlPurgeRecord(purgeId, sourceType, definition?.Name, stages, deletedCount),
                cancellationToken);
        }
        catch (Exception exception)
        {
            logger.LogWarning(exception, "Purge {PurgeId} succeeded but could not be recorded in import history", purgeId);
        }
    }

    private ErrorResponse Error(string message)
        => new()
        {
            Message = message,
            Timestamp = timeProvider.GetUtcNow().UtcDateTime
        };

    private static async IAsyncEnumerable<string> EnumerateKeysAsync(
        IBlobStorageService storage,
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in storage.EnumerateAsync(prefix, cancellationToken))
        {
            yield return item.Key;
        }
    }

    private bool IsStoragePurgeDisabled()
        => environment.IsProduction() && !featureFlags.Value.EtlStoragePurgeEnabled;

    private static string Normalise(string? value, string defaultValue)
        => string.IsNullOrWhiteSpace(value) ? defaultValue : value.Trim().ToLowerInvariant();

    private sealed record StorageTarget(IBlobStorageService Storage, StorageScope Scope, string DisplayFolder);

    /// <summary>What a stage operation works on: the prefixes to work under, and - where the prefixes
    /// alone are broader than the request - which of the keys found under them belong to it.</summary>
    private sealed record StorageScope(IReadOnlyList<string> Prefixes, Func<string, bool>? Matches);
}
