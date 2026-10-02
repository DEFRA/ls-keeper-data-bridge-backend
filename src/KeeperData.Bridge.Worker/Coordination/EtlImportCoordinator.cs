using KeeperData.Core;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Locking;
using KeeperData.Core.Pipeline;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Accepts an ETL import: takes the lock, records the import as queued, and hands the run to
/// the background. Returns as soon as the id exists, so the caller polls for the outcome.
/// </summary>
public sealed class EtlImportCoordinator(
    ILogger<EtlImportCoordinator> logger,
    IDistributedLock distributedLock,
    ILockRenewingRunner runner,
    IServiceScopeFactory scopeFactory,
    IEtlImportStatusStore statusStore,
    IEtlPipelineStorageProvider storageProvider,
    IOptions<EtlImportOptions> options) : IEtlImportCoordinator
{
    private readonly EtlImportOptions _options = options.Value;

    public async Task<EtlImportStartResult> StartAsync(
        string sourceType,
        string? dataset,
        bool rebuild = false,
        CancellationToken cancellationToken = default)
    {
        var importId = Guid.NewGuid();

        var @lock = await distributedLock.TryAcquireAsync(_options.LockName, _options.LockDuration, cancellationToken);

        if (@lock is null)
        {
            var inFlight = await statusStore.GetInFlightAsync(cancellationToken);

            logger.LogInformation(
                "ETL import rejected, {LockName} is held (inFlightImportId={inFlightImportId})",
                _options.LockName,
                inFlight?.ImportId);

            return EtlImportStartResult.Conflict(inFlight?.ImportId);
        }

        if (rebuild && await ClearForRebuildAsync(cancellationToken) is { } failure)
        {
            await @lock.DisposeAsync();
            return failure;
        }

        // Written before the run starts so a poll immediately after the response finds the import,
        // and so the record survives this process dying.
        await statusStore.CreateQueuedAsync(importId, sourceType, dataset, cancellationToken);

        logger.LogInformation(
            "ETL import accepted (importId={importId}, sourceType={sourceType}, dataset={dataset})",
            importId,
            sourceType,
            dataset ?? "all");

        // Lock ownership passes to the runner, which disposes it when the background run ends.
        runner.StartInBackground(
            @lock,
            new LockRenewalSettings(_options.LockName, _options.RenewalInterval, _options.RenewalExtension),
            importId,
            token => RunPipelineAsync(importId, sourceType, dataset, token),
            // A PipelineExecutionException means the pipeline observer already recorded the failure
            // with the richer summary and detail; writing again would clobber it.
            onFailure: exception => exception is PipelineExecutionException
                ? Task.CompletedTask
                : statusStore.MarkFailedAsync(importId, Summarise(exception), Detail(exception), CancellationToken.None),
            cancellationToken);

        return EtlImportStartResult.Started(importId);
    }

    /// <summary>Clears every stage so the run rebuilds from the source files. Runs under the import
    /// lock and before the run is queued, so it cannot interleave with a stage writing.</summary>
    private async Task<EtlImportStartResult?> ClearForRebuildAsync(CancellationToken cancellationToken)
    {
        var stages = EtlStageCascade.Stages;

        logger.LogWarning("ETL rebuild requested: clearing {Stages}", EtlStageCascade.Names);

        var cleared = await EtlStageCascade.ClearAsync(storageProvider, stages, cancellationToken: cancellationToken);

        if (cleared.ClearedStages.Count > 0)
        {
            await statusStore.RecordPurgeAsync(
                new EtlPurgeRecord(Guid.NewGuid(), BlobStorageSources.External, null, cleared.ClearedStages, cleared.Deleted),
                cancellationToken);
        }

        if (cleared.Error is not null)
        {
            logger.LogError(
                "ETL rebuild could not clear stage storage: {Error}. Cleared {ClearedStages} first ({DeletedCount} object(s))",
                cleared.Error,
                cleared.ClearedStages.Count == 0 ? "nothing" : string.Join(", ", cleared.ClearedStages),
                cleared.Deleted);

            return EtlImportStartResult.RebuildFailed(cleared.Error, cleared.ClearedStages);
        }

        logger.LogInformation("ETL rebuild cleared {DeletedCount} object(s)", cleared.Deleted);

        return null;
    }

    private async Task RunPipelineAsync(Guid importId, string sourceType, string? dataset, CancellationToken cancellationToken)
    {
        // The run outlives the request that started it, so it gets a scope of its own rather than
        // borrowing the request's and using it after disposal.
        await using var scope = scopeFactory.CreateAsyncScope();

        var pipeline = scope.ServiceProvider.GetRequiredService<IEtlPipelineFactory>().Create();
        var executor = scope.ServiceProvider.GetRequiredService<IPipelineExecutor>();
        var context = new EtlPipelineContext(importId, sourceType, dataset);

        // Status for the run itself is written by the pipeline's status observer; this only has to
        // report failures that happen outside the pipeline (lock loss, shutdown).
        await executor.RunAsync(pipeline, context, cancellationToken);
    }

    private static string Summarise(Exception exception)
        => $"{Innermost(exception).GetType().Name}: {Innermost(exception).Message}";

    /// <summary>Failures reaching here happened outside the pipeline - lock loss, shutdown - so the
    /// detail can only carry what type of failure it was.</summary>
    private static EtlImportErrorDetail Detail(Exception exception)
        => new() { Type = Innermost(exception).GetType().Name };

    private static Exception Innermost(Exception exception)
    {
        var cause = exception;

        while (cause.InnerException is not null)
        {
            cause = cause.InnerException;
        }

        return cause;
    }
}
