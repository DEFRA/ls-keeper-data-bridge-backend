using FluentAssertions;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Tests.Unit.TestSupport;
using Moq;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Locks the concrete ETL pipeline's stage lineup and order. The GetStageNames()
/// mechanism itself is unit-tested in PipelineFrameworkTests; here we only assert that this
/// factory wires the expected stages, in this order, with decrypt in position two.</summary>
public class EtlPipelineFactoryTests
{
    [Fact]
    public void Create_wires_the_expected_stages_in_order()
    {
        var factory = new EtlPipelineFactory(
            Mock.Of<IExternalCatalogueServiceFactory>(),
            AutoMocked.Instance<DecryptStage>());

        factory.Create().GetStageNames().Should().Equal(
            "discover",
            "decrypt",
            "normalise",
            "snapshot",
            "load-duckdb");
    }
}
