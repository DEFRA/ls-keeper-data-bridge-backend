using KeeperData.Core;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.Locking;
using KeeperData.Core.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Accepts a file-based import: takes the lock, records the import as queued, and hands the run to
/// the background. Returns as soon as the id exists, so the caller polls for the outcome.
/// </summary>
public sealed class FileBasedImportCoordinator(
    ILogger<FileBasedImportCoordinator> logger,
    IDistributedLock distributedLock,
    ILockRenewingRunner runner,
    IServiceScopeFactory scopeFactory,
    IEtlImportStatusStore statusStore,
    IOptions<FileBasedImportOptions> options) : IFileBasedImportCoordinator
{
    private readonly FileBasedImportOptions _options = options.Value;

    public async Task<FileBasedImportStartResult> StartAsync(string sourceType, string? dataset, CancellationToken cancellationToken = default)
    {
        var importId = Guid.NewGuid();

        var @lock = await distributedLock.TryAcquireAsync(_options.LockName, _options.LockDuration, cancellationToken);

        if (@lock is null)
        {
            var inFlight = await statusStore.GetInFlightAsync(cancellationToken);

            logger.LogInformation(
                "File-based import rejected, {LockName} is held (inFlightImportId={inFlightImportId})",
                _options.LockName,
                inFlight?.ImportId);

            return FileBasedImportStartResult.Conflict(inFlight?.ImportId);
        }

        // Written before the run starts so a poll immediately after the response finds the import,
        // and so the record survives this process dying.
        await statusStore.CreateQueuedAsync(importId, sourceType, dataset, cancellationToken);

        logger.LogInformation(
            "File-based import accepted (importId={importId}, sourceType={sourceType}, dataset={dataset})",
            importId,
            sourceType,
            dataset ?? "all");

        // Lock ownership passes to the runner, which disposes it when the background run ends.
        runner.StartInBackground(
            @lock,
            new LockRenewalSettings(_options.LockName, _options.RenewalInterval, _options.RenewalExtension),
            importId,
            token => RunPipelineAsync(importId, sourceType, dataset, token),
            onFailure: exception => statusStore.MarkFailedAsync(importId, Summarise(exception), CancellationToken.None),
            cancellationToken);

        return FileBasedImportStartResult.Started(importId);
    }

    private async Task RunPipelineAsync(Guid importId, string sourceType, string? dataset, CancellationToken cancellationToken)
    {
        // The run outlives the request that started it, so it gets a scope of its own rather than
        // borrowing the request's and using it after disposal.
        await using var scope = scopeFactory.CreateAsyncScope();

        var pipeline = scope.ServiceProvider.GetRequiredService<IEtlPipelineFactory>().Create();
        var executor = scope.ServiceProvider.GetRequiredService<IPipelineExecutor>();
        var context = new EtlPipelineContext(importId, sourceType, EtlConstants.DefaultLookbackDays, dataset);

        // Status for the run itself is written by the pipeline's status observer; this only has to
        // report failures that happen outside the pipeline (lock loss, shutdown).
        await executor.RunAsync(pipeline, context, cancellationToken);
    }

    private static string Summarise(Exception exception)
    {
        var cause = exception;

        while (cause.InnerException is not null)
        {
            cause = cause.InnerException;
        }

        return $"{cause.GetType().Name}: {cause.Message}";
    }
}
