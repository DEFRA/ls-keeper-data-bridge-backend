using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Bridge.Worker.Tasks;
using KeeperData.Core.Locking;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Coordination
{
    public class IngestionRunCoordinatorTests
    {
        private readonly Mock<IDistributedLock> _distributedLock = new();
        private readonly Mock<ITaskProcessBulkFiles> _legacyImport = new();
        private readonly Mock<IHostApplicationLifetime> _lifetime = new();
        private readonly IngestionRunCoordinator _sut;

        public IngestionRunCoordinatorTests()
        {
            _lifetime.Setup(l => l.ApplicationStopping).Returns(CancellationToken.None);

            _sut = new IngestionRunCoordinator(
                Mock.Of<ILogger<IngestionRunCoordinator>>(),
                _distributedLock.Object,
                _legacyImport.Object,
                _lifetime.Object,
                Options.Create(new IngestionRunOptions()));
        }

        [Fact]
        public async Task RunAsync_WhenLockNotAcquired_DoesNotRunImport()
        {
            _distributedLock
                .Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((IDistributedLockHandle?)null);

            await _sut.RunAsync(CancellationToken.None);

            _legacyImport.Verify(
                x => x.RunImportAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task RunAsync_WhenLockAcquired_RunsImportOnce()
        {
            var handle = new Mock<IDistributedLockHandle>();
            _distributedLock
                .Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(handle.Object);

            await _sut.RunAsync(CancellationToken.None);

            _legacyImport.Verify(
                x => x.RunImportAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_WhenLockNotAcquired_ReturnsNullAndDoesNotRunImport()
        {
            _distributedLock
                .Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((IDistributedLockHandle?)null);

            var result = await _sut.StartAsync("external", CancellationToken.None);

            Assert.Null(result);
            _legacyImport.Verify(
                x => x.RunImportAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task StartAsync_WhenLockAcquired_ReturnsRunId()
        {
            var handle = new Mock<IDistributedLockHandle>();
            _distributedLock
                .Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(handle.Object);

            var result = await _sut.StartAsync("external", CancellationToken.None);

            // StartAsync's contract is synchronous: acquire the lock and return the run id.
            // The background run itself is fire-and-forget and covered by integration tests.
            Assert.NotNull(result);
        }
    }
}
