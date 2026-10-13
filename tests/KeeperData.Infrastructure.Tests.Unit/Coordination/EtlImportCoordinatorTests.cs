using FluentAssertions;
using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Locking;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Coordination;

/// <summary>The coordinator decides whether an ETL import happens at all: it takes the lock,
/// records the import before any work starts, and refuses a second concurrent run.</summary>
public class EtlImportCoordinatorTests
{
    private readonly Mock<IDistributedLock> _distributedLock = new();
    private readonly Mock<ILockRenewingRunner> _runner = new();
    private readonly Mock<IEtlImportStatusStore> _statusStore = new();
    private readonly Mock<IEtlPipelineStorageProvider> _storageProvider = new();
    private readonly EtlImportOptions _options = new();
    private readonly EtlImportCoordinator _sut;

    public EtlImportCoordinatorTests()
    {
        _sut = new EtlImportCoordinator(
            Mock.Of<ILogger<EtlImportCoordinator>>(),
            _distributedLock.Object,
            _runner.Object,
            new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
            _statusStore.Object,
            _storageProvider.Object,
            Options.Create(_options));
    }

    private void SetupLock(IDistributedLockHandle? handle) =>
        _distributedLock
            .Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(handle);

    private Mock<IBlobStorageService> SetupFolders(ClearDownResult? result = null, Exception? failure = null)
    {
        var folder = new Mock<IBlobStorageService>();

        var setup = folder.Setup(f => f.DeleteByPrefixAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()));

        if (failure is not null)
        {
            setup.ThrowsAsync(failure);
        }
        else
        {
            setup.ReturnsAsync(result ?? new ClearDownResult { DeletedKeys = [], TotalDeleted = 0 });
        }

        _storageProvider.Setup(p => p.ForFolder(It.IsAny<string>())).Returns(folder.Object);

        return folder;
    }

    [Fact]
    public async Task StartAsync_WhenRebuilding_ClearsEveryStageBeforeTheRunIsQueued()
    {
        SetupLock(Mock.Of<IDistributedLockHandle>());
        var folder = SetupFolders(new ClearDownResult { DeletedKeys = ["a"], TotalDeleted = 7 });

        var result = await _sut.StartAsync("external", null, rebuild: true, CancellationToken.None);

        result.Accepted.Should().BeTrue();

        folder.Verify(
            f => f.DeleteByPrefixAsync(string.Empty, It.IsAny<CancellationToken>()),
            Times.Exactly(EtlStageCascade.Stages.Length),
            "a rebuild clears every stage, not only the one it starts from");

        _statusStore.Verify(
            s => s.RecordPurgeAsync(
                It.Is<EtlPurgeRecord>(p => p.Stages.Count == EtlStageCascade.Stages.Length && p.DeletedCount == 7 * EtlStageCascade.Stages.Length),
                It.IsAny<CancellationToken>()),
            Times.Once,
            "the wipe belongs in the same history as the run that follows it");
    }

    [Fact]
    public async Task StartAsync_WhenNotRebuilding_LeavesStageStorageAlone()
    {
        SetupLock(Mock.Of<IDistributedLockHandle>());
        var folder = SetupFolders();

        await _sut.StartAsync("external", null, rebuild: false, CancellationToken.None);

        folder.Verify(f => f.DeleteByPrefixAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>A half-cleared staging area with a run on top of it would produce a confidently wrong
    /// read model, so a clear that cannot finish stops the run rather than proceeding.</summary>
    [Fact]
    public async Task StartAsync_WhenRebuildCannotClear_ReportsWhyAndStartsNothing()
    {
        SetupLock(Mock.Of<IDistributedLockHandle>());
        SetupFolders(failure: new IOException("the file is in use"));

        var result = await _sut.StartAsync("external", null, rebuild: true, CancellationToken.None);

        result.Accepted.Should().BeFalse();
        result.RebuildError.Should().Contain("the file is in use");

        _statusStore.Verify(
            s => s.CreateQueuedAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);

        _runner.Verify(
            r => r.StartInBackground(
                It.IsAny<IDistributedLockHandle>(),
                It.IsAny<LockRenewalSettings>(),
                It.IsAny<Guid>(),
                It.IsAny<Func<CancellationToken, Task>>(),
                It.IsAny<Func<Exception, Task>>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    /// <summary>Stopping is not undoing. The stages before the failure are already empty, and a wipe
    /// that leaves no trace in the history is worse than one that fails loudly.</summary>
    [Fact]
    public async Task StartAsync_WhenRebuildStopsPartWayThrough_RecordsWhatItAlreadyCleared()
    {
        SetupLock(Mock.Of<IDistributedLockHandle>());

        var cleared = new Mock<IBlobStorageService>();
        cleared
            .Setup(f => f.DeleteByPrefixAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ClearDownResult { DeletedKeys = ["a"], TotalDeleted = 3 });

        var locked = new Mock<IBlobStorageService>();
        locked
            .Setup(f => f.DeleteByPrefixAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("the file is in use"));

        _storageProvider
            .Setup(p => p.ForFolder(It.IsAny<string>()))
            .Returns((string folder) => folder == EtlPipelineFolders.Views ? locked.Object : cleared.Object);

        var result = await _sut.StartAsync("external", null, rebuild: true, CancellationToken.None);

        result.Accepted.Should().BeFalse();
        result.ClearedStages.Should().Equal(EtlStageCascade.Stages[..^1]);

        _statusStore.Verify(
            s => s.RecordPurgeAsync(
                It.Is<EtlPurgeRecord>(p =>
                    p.Stages.Count == EtlStageCascade.Stages.Length - 1
                    && p.DeletedCount == 3 * (EtlStageCascade.Stages.Length - 1)),
                It.IsAny<CancellationToken>()),
            Times.Once,
            "the partial wipe still happened, so it belongs in the history");
    }

    [Fact]
    public async Task StartAsync_WhenLockAcquired_RecordsTheImportAndStartsItInTheBackground()
    {
        SetupLock(Mock.Of<IDistributedLockHandle>());

        var result = await _sut.StartAsync("external", "sam_cph_holdings", false, CancellationToken.None);

        result.Accepted.Should().BeTrue();

        _statusStore.Verify(
            s => s.CreateQueuedAsync(result.ImportId!.Value, "external", "sam_cph_holdings", It.IsAny<CancellationToken>()),
            Times.Once);

        _runner.Verify(
            r => r.StartInBackground(
                It.IsAny<IDistributedLockHandle>(),
                It.IsAny<LockRenewalSettings>(),
                result.ImportId!.Value,
                It.IsAny<Func<CancellationToken, Task>>(),
                It.IsAny<Func<Exception, Task>>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task StartAsync_WhenLockHeld_IsRejectedAndReportsTheRunItCollidedWith()
    {
        var inFlight = Guid.NewGuid();

        SetupLock(null);
        _statusStore
            .Setup(s => s.GetInFlightAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new EtlImportDocument
            {
                ImportId = inFlight,
                Status = nameof(EtlImportStatus.Running),
                SourceType = "external"
            });

        var result = await _sut.StartAsync("external", null, false, CancellationToken.None);

        result.Accepted.Should().BeFalse();
        result.InFlightImportId.Should().Be(inFlight);

        _statusStore.Verify(
            s => s.CreateQueuedAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task StartAsync_WhenLockHeldByARunWithNoStatus_IsStillRejected()
    {
        SetupLock(null);
        _statusStore
            .Setup(s => s.GetInFlightAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((EtlImportDocument?)null);

        var result = await _sut.StartAsync("external", null, false, CancellationToken.None);

        result.Accepted.Should().BeFalse();
        result.InFlightImportId.Should().BeNull();
    }

    [Fact]
    public async Task StartAsync_UsesItsOwnLockRatherThanTheLegacyImportLock()
    {
        SetupLock(Mock.Of<IDistributedLockHandle>());

        await _sut.StartAsync("external", null, false, CancellationToken.None);

        _options.LockName.Should().NotBe(new IngestionRunOptions().LockName);
        _distributedLock.Verify(
            l => l.TryAcquireAsync(_options.LockName, _options.LockDuration, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task StartAsync_WhenTheBackgroundRunFailsOutsideThePipeline_MarksTheImportFailed()
    {
        SetupLock(Mock.Of<IDistributedLockHandle>());

        var onFailure = CaptureOnFailure();
        var result = await _sut.StartAsync("external", null, false, CancellationToken.None);

        await onFailure(new InvalidOperationException("Failed to renew lock for EtlImportRun"));

        _statusStore.Verify(
            s => s.MarkFailedAsync(
                result.ImportId!.Value,
                "InvalidOperationException: Failed to renew lock for EtlImportRun",
                It.Is<EtlImportErrorDetail?>(d => d!.Type == "InvalidOperationException"),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    /// <summary>A PipelineExecutionException means the executor already notified the status observer,
    /// which recorded the failure with the richer summary and detail; writing again would clobber it.</summary>
    [Fact]
    public async Task StartAsync_WhenThePipelineAlreadyReportedTheFailure_LeavesTheStatusAlone()
    {
        SetupLock(Mock.Of<IDistributedLockHandle>());

        var onFailure = CaptureOnFailure();
        var result = await _sut.StartAsync("external", null, false, CancellationToken.None);

        await onFailure(new PipelineExecutionException(
            "Pipeline failed after 10ms.",
            new InvalidOperationException("snapshot timestamp could not be parsed")));

        _statusStore.Verify(
            s => s.MarkFailedAsync(
                It.IsAny<Guid>(),
                It.IsAny<string>(),
                It.IsAny<EtlImportErrorDetail?>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    /// <summary>The callback the coordinator registers with the runner, so a test can invoke it with
    /// the kind of fault the background run would have died with.</summary>
    private Func<Exception, Task> CaptureOnFailure()
    {
        Func<Exception, Task>? onFailure = null;

        _runner
            .Setup(r => r.StartInBackground(
                It.IsAny<IDistributedLockHandle>(),
                It.IsAny<LockRenewalSettings>(),
                It.IsAny<Guid>(),
                It.IsAny<Func<CancellationToken, Task>>(),
                It.IsAny<Func<Exception, Task>>(),
                It.IsAny<CancellationToken>()))
            .Callback((IDistributedLockHandle _, LockRenewalSettings _, Guid _, Func<CancellationToken, Task> _, Func<Exception, Task>? failure, CancellationToken _) => onFailure = failure);

        return exception => onFailure!(exception);
    }
}

