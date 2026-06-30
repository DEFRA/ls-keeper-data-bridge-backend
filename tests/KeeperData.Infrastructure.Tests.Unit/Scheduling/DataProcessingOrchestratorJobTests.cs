using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Bridge.Worker.Jobs;
using Microsoft.Extensions.Logging;
using Moq;
using Quartz;

namespace KeeperData.Infrastructure.Tests.Unit.Scheduling
{
    public class DataProcessingOrchestratorJobTests
    {
        private readonly Mock<ILogger<ImportBulkFilesJob>> _loggerMock;
        private readonly Mock<IIngestionRunCoordinator> _coordinatorMock;
        private readonly Mock<IJobExecutionContext> _jobExecutionContextMock;

        private readonly ImportBulkFilesJob _sut;

        public DataProcessingOrchestratorJobTests()
        {
            _loggerMock = new Mock<ILogger<ImportBulkFilesJob>>();
            _coordinatorMock = new Mock<IIngestionRunCoordinator>();
            _jobExecutionContextMock = new Mock<IJobExecutionContext>();

            var cts = new CancellationTokenSource();
            _jobExecutionContextMock.Setup(c => c.CancellationToken).Returns(cts.Token);

            _sut = new ImportBulkFilesJob(
                _coordinatorMock.Object,
                _loggerMock.Object);
        }

        [Fact]
        public async Task Execute_WhenSuccessful_CallsCoordinatorOnce()
        {
            _coordinatorMock.Setup(x => x.RunAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

            await _sut.Execute(_jobExecutionContextMock.Object);

            _coordinatorMock.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Execute_WhenCoordinatorFails_ThrowsException()
        {
            _coordinatorMock.Setup(x => x.RunAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("Invalid operation exception"));

            async Task act() => await _sut.Execute(_jobExecutionContextMock.Object);

            await Assert.ThrowsAsync<InvalidOperationException>(act);

            _coordinatorMock.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Execute_WhenCancellationIsRequested_StopsProcessingAndThrows()
        {
            var cts = new CancellationTokenSource();
            _jobExecutionContextMock.Setup(c => c.CancellationToken).Returns(cts.Token);

            _coordinatorMock
                .Setup(x => x.RunAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(async (token) => await Task.Delay(100, token));

            var executionTask = _sut.Execute(_jobExecutionContextMock.Object);
            cts.Cancel();

            await Assert.ThrowsAsync<TaskCanceledException>(() => executionTask);
        }
    }
}
