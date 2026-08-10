using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Worker.Tasks;
using KeeperData.Core.Locking;
using Microsoft.Extensions.Options;

namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Runs the legacy Mongo import while keeping the run lock alive. The file-based pipeline has its
/// own trigger and executor; this path is unchanged by it.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Thin adapter over the shared lock-renewing runner; exercised by integration tests.")]
public sealed class IngestionRunExecutor(
    ILockRenewingRunner runner,
    ITaskProcessBulkFiles legacyImport,
    IOptions<IngestionRunOptions> options) : IIngestionRunExecutor
{
    private readonly IngestionRunOptions _options = options.Value;

    public void StartInBackground(IDistributedLockHandle lockHandle, Guid runId, string sourceType, CancellationToken cancellationToken)
        => runner.StartInBackground(
            lockHandle,
            Settings,
            runId,
            token => legacyImport.RunImportAsync(runId, sourceType, token),
            onFailure: null,
            cancellationToken);

    public Task RunWithRenewalAsync(IDistributedLockHandle lockHandle, Guid runId, string sourceType, CancellationToken cancellationToken)
        => runner.RunAsync(
            lockHandle,
            Settings,
            runId,
            token => legacyImport.RunImportAsync(runId, sourceType, token),
            cancellationToken);

    private LockRenewalSettings Settings
        => new(_options.LockName, _options.RenewalInterval, _options.RenewalExtension);
}
