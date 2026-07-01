using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Core.Locking;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Coordination
{
    public class IngestionRunCoordinatorTests
    {
        private readonly Mock<IDistributedLock> _distributedLock = new();
        private readonly Mock<IIngestionRunExecutor> _executor = new();
        private readonly IngestionRunCoordinator _sut;

        public IngestionRunCoordinatorTests()
        {
            _sut = new IngestionRunCoordinator(
                Mock.Of<ILogger<IngestionRunCoordinator>>(),
                _distributedLock.Object,
                _executor.Object,
                Options.Create(new IngestionRunOptions()));
        }

        private void SetupLock(IDistributedLockHandle? handle) =>
            _distributedLock
                .Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(handle);

        [Fact]
        public async Task RunAsync_WhenLockNotAcquired_DoesNotExecute()
        {
            SetupLock(null);

            await _sut.RunAsync(CancellationToken.None);

            _executor.Verify(
                x => x.RunWithRenewalAsync(It.IsAny<IDistributedLockHandle>(), It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task RunAsync_WhenLockAcquired_ExecutesOnce()
        {
            SetupLock(Mock.Of<IDistributedLockHandle>());

            await _sut.RunAsync(CancellationToken.None);

            _executor.Verify(
                x => x.RunWithRenewalAsync(It.IsAny<IDistributedLockHandle>(), It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_WhenLockNotAcquired_ReturnsNullAndDoesNotExecute()
        {
            SetupLock(null);

            var result = await _sut.StartAsync("external", CancellationToken.None);

            Assert.Null(result);
            _executor.Verify(
                x => x.StartInBackground(It.IsAny<IDistributedLockHandle>(), It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task StartAsync_WhenLockAcquired_ReturnsRunIdAndStartsBackgroundRun()
        {
            SetupLock(Mock.Of<IDistributedLockHandle>());

            var result = await _sut.StartAsync("external", CancellationToken.None);

            Assert.NotNull(result);
            _executor.Verify(
                x => x.StartInBackground(It.IsAny<IDistributedLockHandle>(), It.IsAny<Guid>(), "external", It.IsAny<CancellationToken>()),
                Times.Once);
        }
    }
}
