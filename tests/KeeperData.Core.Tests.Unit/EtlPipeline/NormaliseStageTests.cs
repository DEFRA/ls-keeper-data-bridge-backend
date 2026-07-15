using FluentAssertions;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Normalise. Input: RawFileSet. Output: NormalisedFileSet.
/// OWNER: add your normaliser to NormaliseStage, then assert Parquet lands in normalised/.</summary>
public class NormaliseStageTests
{
    private static Task<List<NormalisedFileSet>> RunAsync(params RawFileSet[] inputs) =>
        StageRunner.RunAsync(new NormaliseStage(), inputs);

    [Fact]
    public async Task Produces_one_normalised_file_set_per_input()
    {
        var output = await RunAsync(
            new RawFileSet(StageRunner.Definition("SAM_CPH")),
            new RawFileSet(StageRunner.Definition("CTS_KEEPER")));

        output.Should().HaveCount(2);
    }

    [Fact]
    public async Task Produces_nothing_for_an_empty_input()
    {
        var output = await RunAsync();

        output.Should().BeEmpty();
    }
}
