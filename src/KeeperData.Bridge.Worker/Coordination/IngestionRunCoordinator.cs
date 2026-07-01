using KeeperData.Core.Locking;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Owns the run lock (previously owned by the legacy task) and decides whether a run proceeds.
/// The actual execution (lock renewal, background dispatch) is delegated to
/// <see cref="IIngestionRunExecutor"/>, keeping this class free of timing/threading concerns.
/// </summary>
public sealed class IngestionRunCoordinator(
    ILogger<IngestionRunCoordinator> logger,
    IDistributedLock distributedLock,
    IIngestionRunExecutor executor,
    IOptions<IngestionRunOptions> options) : IIngestionRunCoordinator
{
    private readonly IngestionRunOptions _options = options.Value;

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var runId = Guid.NewGuid();
        logger.LogInformation("Attempting to acquire lock for {LockName} (runId={runId}).", _options.LockName, runId);

        await using var @lock = await distributedLock.TryAcquireAsync(_options.LockName, _options.LockDuration, cancellationToken);
        if (@lock is null)
        {
            logger.LogInformation("Could not acquire lock for {LockName}, another run is likely in progress (runId={runId}).", _options.LockName, runId);
            return;
        }

        logger.LogInformation("Lock acquired for {LockName}. Run started at {startTime} (runId={runId}).", _options.LockName, DateTime.UtcNow, runId);
        await executor.RunWithRenewalAsync(@lock, runId, BlobStorageSources.External, cancellationToken);
    }

    public async Task<Guid?> StartAsync(string sourceType, CancellationToken cancellationToken = default)
    {
        var runId = Guid.NewGuid();
        logger.LogInformation("Attempting to acquire lock for {LockName} with sourceType={sourceType} (runId={runId}).", _options.LockName, sourceType, runId);

        var @lock = await distributedLock.TryAcquireAsync(_options.LockName, _options.LockDuration, cancellationToken);
        if (@lock is null)
        {
            logger.LogInformation("Could not acquire lock for {LockName}, another run is likely in progress (runId={runId}).", _options.LockName, runId);
            return null;
        }

        logger.LogInformation("Lock acquired for {LockName}. Starting run in background with sourceType={sourceType} (runId={runId}).", _options.LockName, sourceType, runId);

        // Hand lock ownership to the executor; it disposes the handle when the background run ends.
        executor.StartInBackground(@lock, runId, sourceType, cancellationToken);

        return runId;
    }
}
