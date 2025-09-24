using KeeperData.Bridge.Worker.Jobs;
using KeeperData.Bridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using Quartz;

namespace KeeperData.Infrastructure.Tests.Unit.Scheduling
{
    public class DataProcessingOrchestratorJobTests
    {
        private readonly Mock<ILogger<ImportBulkFilesJob>> _loggerMock;
        private readonly Mock<ITaskProcessBulkFiles> _taskProcessBulkFilesMock;
        private readonly Mock<IJobExecutionContext> _jobExecutionContextMock;

        private readonly ImportBulkFilesJob _sut;

        public DataProcessingOrchestratorJobTests()
        {
            _loggerMock = new Mock<ILogger<ImportBulkFilesJob>>();
            _taskProcessBulkFilesMock = new Mock<ITaskProcessBulkFiles>();
            _jobExecutionContextMock = new Mock<IJobExecutionContext>();

            var cts = new CancellationTokenSource();
            _jobExecutionContextMock.Setup(c => c.CancellationToken).Returns(cts.Token);

            _sut = new ImportBulkFilesJob(
                _taskProcessBulkFilesMock.Object,
                _loggerMock.Object);
        }

        [Fact]
        public async Task Execute_WhenSuccessful_CallsTasksInSequence()
        {
            var sequence = new MockSequence();
            _taskProcessBulkFilesMock.InSequence(sequence).Setup(x => x.RunAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

            await _sut.Execute(_jobExecutionContextMock.Object);

            _taskProcessBulkFilesMock.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Execute_WhenTaskProcessFails_ThrowsException()
        {
            _taskProcessBulkFilesMock.Setup(x => x.RunAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("Invalid operation exception"));

            async Task act() => await _sut.Execute(_jobExecutionContextMock.Object);

            await Assert.ThrowsAsync<InvalidOperationException>(act);

            _taskProcessBulkFilesMock.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Execute_WhenCancellationIsRequested_StopsProcessingAndThrows()
        {
            var cts = new CancellationTokenSource();
            _jobExecutionContextMock.Setup(c => c.CancellationToken).Returns(cts.Token);

            _taskProcessBulkFilesMock
                .Setup(x => x.RunAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(async (token) => await Task.Delay(100, token));

            // Act
            var executionTask = _sut.Execute(_jobExecutionContextMock.Object);
            cts.Cancel();

            // Assert
            await Assert.ThrowsAsync<TaskCanceledException>(() => executionTask);
        }
    }
}