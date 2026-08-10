using FluentAssertions;
using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.Locking;
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
            Options.Create(_options));
    }

    private void SetupLock(IDistributedLockHandle? handle) =>
        _distributedLock
            .Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(handle);

    [Fact]
    public async Task StartAsync_WhenLockAcquired_RecordsTheImportAndStartsItInTheBackground()
    {
        SetupLock(Mock.Of<IDistributedLockHandle>());

        var result = await _sut.StartAsync("external", "sam_cph_holdings", CancellationToken.None);

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

        var result = await _sut.StartAsync("external", null, CancellationToken.None);

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

        var result = await _sut.StartAsync("external", null, CancellationToken.None);

        result.Accepted.Should().BeFalse();
        result.InFlightImportId.Should().BeNull();
    }

    [Fact]
    public async Task StartAsync_UsesItsOwnLockRatherThanTheLegacyImportLock()
    {
        SetupLock(Mock.Of<IDistributedLockHandle>());

        await _sut.StartAsync("external", null, CancellationToken.None);

        _options.LockName.Should().NotBe(new IngestionRunOptions().LockName);
        _distributedLock.Verify(
            l => l.TryAcquireAsync(_options.LockName, _options.LockDuration, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task StartAsync_WhenTheBackgroundRunFailsOutsideThePipeline_MarksTheImportFailed()
    {
        SetupLock(Mock.Of<IDistributedLockHandle>());

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

        var result = await _sut.StartAsync("external", null, CancellationToken.None);

        await onFailure!(new InvalidOperationException("Failed to renew lock for FileBasedEtlRun"));

        _statusStore.Verify(
            s => s.MarkFailedAsync(
                result.ImportId!.Value,
                "InvalidOperationException: Failed to renew lock for FileBasedEtlRun",
                It.IsAny<CancellationToken>()),
            Times.Once);
    }
}
