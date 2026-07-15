using FluentAssertions;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Snapshot. Input: NormalisedFileSet. Output: SnapshotFile.
/// OWNER: implement the fold, then assert one snapshot per dataset with deltas applied.</summary>
public class SnapshotStageTests
{
    private static Task<List<SnapshotFile>> RunAsync(params NormalisedFileSet[] inputs) =>
        StageRunner.RunAsync(new SnapshotStage(), inputs);

    [Fact]
    public async Task Produces_one_snapshot_per_input()
    {
        var output = await RunAsync(
            new NormalisedFileSet(StageRunner.Definition("SAM_CPH")),
            new NormalisedFileSet(StageRunner.Definition("CTS_KEEPER")));

        output.Should().HaveCount(2);
    }

    [Fact]
    public async Task Produces_nothing_for_an_empty_input()
    {
        var output = await RunAsync();

        output.Should().BeEmpty();
    }
}
