using KeeperData.Bridge.Worker.Tasks.Implementations;
using KeeperData.Core.ETL.Impl;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Tasks
{
    public class TaskProcessBulkFilesTests
    {
        [Fact]
        public async Task RunImportAsync_InvokesOrchestratorWithGivenArguments()
        {
            var orchestrator = new Mock<IImportOrchestrator>();
            var sut = new TaskProcessBulkFiles(Mock.Of<ILogger<TaskProcessBulkFiles>>(), orchestrator.Object);

            var importId = Guid.NewGuid();

            await sut.RunImportAsync(importId, "external", CancellationToken.None);

            orchestrator.Verify(
                o => o.StartAsync(importId, "external", It.IsAny<CancellationToken>()),
                Times.Once);
        }
    }
}
