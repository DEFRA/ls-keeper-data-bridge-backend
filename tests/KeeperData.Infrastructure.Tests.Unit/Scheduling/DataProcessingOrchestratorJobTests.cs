using KeeperData.Bridge.Worker.Jobs;
using KeeperData.Bridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using Quartz;

namespace KeeperData.Infrastructure.Tests.Unit.Scheduling
{
    public class DataProcessingOrchestratorJobTests
    {
        private readonly Mock<ILogger<DataProcessingOrchestratorJob>> _mockLogger;
        private readonly Mock<ITaskDownload> _mockTaskDownload;
        private readonly Mock<ITaskProcess> _mockTaskProcess;
        private readonly Mock<IJobExecutionContext> _mockContext;
        private readonly DataProcessingOrchestratorJob _sut;

        public DataProcessingOrchestratorJobTests()
        {
            _mockLogger = new Mock<ILogger<DataProcessingOrchestratorJob>>();
            _mockTaskDownload = new Mock<ITaskDownload>();
            _mockTaskProcess = new Mock<ITaskProcess>();
            _mockContext = new Mock<IJobExecutionContext>();

            var cts = new CancellationTokenSource();
            _mockContext.Setup(c => c.CancellationToken).Returns(cts.Token);

            _sut = new DataProcessingOrchestratorJob(
                _mockLogger.Object,
                _mockTaskDownload.Object,
                _mockTaskProcess.Object);
        }

        [Fact]
        public async Task Execute_WhenSuccessful_CallsTasksInSequence()
        {
            var sequence = new MockSequence();
            _mockTaskDownload.InSequence(sequence).Setup(x => x.RunAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            _mockTaskProcess.InSequence(sequence).Setup(x => x.RunAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

            await _sut.Execute(_mockContext.Object);

            // Verify calls are in the correct order.
            _mockTaskDownload.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
            _mockTaskProcess.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Execute_WhenTaskAFails_DoesNotCallTaskBAndThrows()
        {
            var expectedException = new InvalidOperationException("Task A failed!");
            _mockTaskDownload.Setup(x => x.RunAsync(It.IsAny<CancellationToken>())).ThrowsAsync(expectedException);

            var action = async () => await _sut.Execute(_mockContext.Object);

            await Assert.ThrowsAsync<InvalidOperationException>(action);
            _mockTaskProcess.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task Execute_WhenTaskProcessFails_ThrowsException()
        {
            var expectedException = new InvalidOperationException("Task Process failed!");

            // Task A succeeds
            _mockTaskDownload.Setup(x => x.RunAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

            // Task B fails
            _mockTaskProcess.Setup(x => x.RunAsync(It.IsAny<CancellationToken>())).ThrowsAsync(expectedException);

            var action = async () => await _sut.Execute(_mockContext.Object);

            await Assert.ThrowsAsync<InvalidOperationException>(action);

            _mockTaskDownload.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
        }


        [Fact]
        public async Task Execute_WhenCancellationIsRequested_StopsProcessingAndThrows()
        {
            var cts = new CancellationTokenSource();

            _mockContext.Setup(c => c.CancellationToken).Returns(cts.Token);

            _mockTaskDownload
                .Setup(x => x.RunAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(async (token) => {
                    await Task.Delay(100, token);
                });

            // Act
            var executionTask = _sut.Execute(_mockContext.Object);
            cts.Cancel();

            // Assert
            await Assert.ThrowsAsync<TaskCanceledException>(() => executionTask);

            _mockTaskProcess.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Never);
        }
    }
}
