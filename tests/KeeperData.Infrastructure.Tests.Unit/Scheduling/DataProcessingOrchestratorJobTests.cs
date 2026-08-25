using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Bridge.Worker.Jobs;
using Microsoft.Extensions.Logging;
using Moq;
using Quartz;

namespace KeeperData.Infrastructure.Tests.Unit.Scheduling
{
    // ImportBulkFilesJob dispatches nothing while the old ETL is switched off (commit 58169a3);
    // restore the coordinator-dispatch tests from history when it is switched back on.
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
        public async Task Execute_WhileTheOldEtlIsDisabled_DoesNotStartAnIngestionRun()
        {
            await _sut.Execute(_jobExecutionContextMock.Object);

            _coordinatorMock.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Never);
        }
    }
}
