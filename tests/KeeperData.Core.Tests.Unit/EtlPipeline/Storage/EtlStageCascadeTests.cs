using FluentAssertions;
using KeeperData.Core.EtlPipeline.Storage;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Storage;

/// <summary>Which stages a rebuild clears. Every artefact is written once and skipped if it already
/// exists, so clearing a stage without clearing what follows leaves the later stages reading
/// outputs built from files that are no longer there - these tests are that rule.</summary>
public class EtlStageCascadeTests
{
    [Fact]
    public void Stages_run_in_pipeline_order()
        => EtlStageCascade.Stages.Should().Equal(
            EtlPipelineFolders.Raw,
            EtlPipelineFolders.Normalised,
            EtlPipelineFolders.Optimised,
            EtlPipelineFolders.Snapshots,
            EtlPipelineFolders.Staging,
            EtlPipelineFolders.Views);

    [Fact]
    public void From_a_stage_takes_everything_downstream_of_it()
        => EtlStageCascade.From(EtlPipelineFolders.Optimised).Should().Equal(
            EtlPipelineFolders.Optimised,
            EtlPipelineFolders.Snapshots,
            EtlPipelineFolders.Staging,
            EtlPipelineFolders.Views);

    [Fact]
    public void From_the_last_stage_takes_only_that_stage()
        => EtlStageCascade.From(EtlPipelineFolders.Views).Should().Equal(EtlPipelineFolders.Views);

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void From_nothing_takes_every_stage(string? stage)
        => EtlStageCascade.From(stage).Should().Equal(EtlStageCascade.Stages);

    [Fact]
    public void From_is_not_case_sensitive()
        => EtlStageCascade.From("SNAPSHOTS").Should().Equal(
            EtlPipelineFolders.Snapshots,
            EtlPipelineFolders.Staging,
            EtlPipelineFolders.Views);

    [Fact]
    public void From_an_unknown_stage_takes_nothing()
        // The caller reports this as a bad argument rather than clearing everything, which is what
        // a typo would otherwise do.
        => EtlStageCascade.From("optimized").Should().BeEmpty();

    [Fact]
    public void Names_lists_the_stages_for_an_error_message()
        => EtlStageCascade.Names.Should().Be(string.Join(", ", EtlStageCascade.Stages));
}
