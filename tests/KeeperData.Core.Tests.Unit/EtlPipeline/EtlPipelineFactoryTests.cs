using FluentAssertions;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Guards the stage order and the fluent wiring. Building the definition exercises every
/// fluent extension (Discover/Decrypt/Normalise/Snapshot/LoadDuckDb).</summary>
public class EtlPipelineFactoryTests
{
    [Fact]
    public void Create_defines_every_stage_in_order()
    {
        var factory = new EtlPipelineFactory(
            Mock.Of<IExternalCatalogueServiceFactory>(),
            Mock.Of<SnapshotStage>());

        factory.Create().GetStageNames().Should().Equal(
            "discover",
            "decrypt",
            "normalise",
            "snapshot",
            "load-duckdb");
    }
}
