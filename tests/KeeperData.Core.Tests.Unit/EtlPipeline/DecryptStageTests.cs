using FluentAssertions;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Decrypt. Input: DiscoveredFileSet. Output: RawFileSet.
/// OWNER: add your dependency to DecryptStage, then assert files land in raw/.</summary>
public class DecryptStageTests
{
    private static Task<List<RawFileSet>> RunAsync(params DiscoveredFileSet[] inputs) =>
        StageRunner.RunAsync(new DecryptStage(), inputs);

    [Fact]
    public async Task Produces_one_raw_file_set_per_input()
    {
        var output = await RunAsync(
            StageRunner.DiscoveredSet("SAM_CPH", "SAM_CPH_1.csv"),
            StageRunner.DiscoveredSet("CTS_KEEPER", "CTS_KEEPER_1.csv"));

        output.Should().HaveCount(2);
    }
}
