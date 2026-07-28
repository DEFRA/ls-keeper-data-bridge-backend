using FluentAssertions;
using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging.Abstractions;
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
        var factory = new EtlPipelineFactory(Mock.Of<IExternalCatalogueServiceFactory>(), CreateDecryptStage());

        factory.Create().GetStageNames().Should().Equal(
            "discover",
            "decrypt",
            "normalise",
            "snapshot",
            "load-duckdb");
    }

    private static DecryptStage CreateDecryptStage() => new(
        Mock.Of<IBlobStorageServiceFactory>(),
        Mock.Of<IEtlPipelineStorageProvider>(),
        Mock.Of<IAesCryptoTransform>(),
        Mock.Of<IPasswordSaltService>(),
        NullLogger<DecryptStage>.Instance);
}
