using FluentAssertions;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

public class DiscoverStageTests
{
    private static Task<List<DiscoveredFileSet>> RunAsync(params DiscoveredFile[] inputs) =>
        StageRunner.RunAsync(new DiscoverStage(), inputs);

    [Fact]
    public async Task Groups_the_discovered_files_by_dataset()
    {
        var output = await RunAsync(
            StageRunner.Discovered("SAM_CPH", "SAM_CPH_1.csv"),
            StageRunner.Discovered("CTS_KEEPER", "CTS_KEEPER_1.csv"),
            StageRunner.Discovered("SAM_CPH", "SAM_CPH_2.csv"));

        output.Should().HaveCount(2);
        output.Single(s => s.Definition.Name == "SAM_CPH").Files.Should().HaveCount(2);
        output.Single(s => s.Definition.Name == "CTS_KEEPER").Files.Should().ContainSingle();
    }

    [Fact]
    public async Task Yields_nothing_for_an_empty_input()
    {
        var output = await RunAsync();

        output.Should().BeEmpty();
    }
}
