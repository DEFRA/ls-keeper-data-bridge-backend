using FluentAssertions;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Load. Input: SnapshotFile (all of them). Output: one StagingDatabase.
/// OWNER: add your DuckDB writer, then assert every snapshot becomes a table in the one database.</summary>
public class LoadDuckDbStageTests
{
    private static Task<List<StagingDatabase>> RunAsync(params SnapshotFile[] inputs) =>
        StageRunner.RunAsync(new LoadDuckDbStage(), inputs);

    [Fact]
    public async Task Collapses_all_snapshots_into_a_single_database()
    {
        var output = await RunAsync(
            new SnapshotFile(StageRunner.Definition("SAM_CPH")),
            new SnapshotFile(StageRunner.Definition("CTS_KEEPER")));

        output.Should().ContainSingle();
    }

    [Fact]
    public async Task Still_produces_a_single_database_for_an_empty_input()
    {
        var output = await RunAsync();

        output.Should().ContainSingle();
    }
}
