using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

namespace KeeperData.Core.Tests.Unit.Reports.Cleanse;

public class TimingTreeTests
{
    [Fact]
    public void Track_SingleLeaf_CreatesNodeWithCorrectMs()
    {
        var tree = new TimingTree();

        tree.Track("fetching", 500);

        var snapshot = tree.Snapshot();
        snapshot.Name.Should().Be("total");
        snapshot.ElapsedMs.Should().Be(500);
        snapshot.Children.Should().HaveCount(1);
        snapshot.Children![0].Name.Should().Be("fetching");
        snapshot.Children![0].ElapsedMs.Should().Be(500);
        snapshot.Children![0].Children.Should().BeNull();
    }

    [Fact]
    public void Track_MultipleCalls_AccumulatesMs()
    {
        var tree = new TimingTree();

        tree.Track("fetching", 100);
        tree.Track("fetching", 200);
        tree.Track("fetching", 300);

        var snapshot = tree.Snapshot();
        snapshot.Children![0].ElapsedMs.Should().Be(600);
    }

    [Fact]
    public void Track_NestedPath_CreatesHierarchy()
    {
        var tree = new TimingTree();

        tree.Track("CTS Pump/fetching", 300);
        tree.Track("CTS Pump/record_processing", 500);

        var snapshot = tree.Snapshot();
        snapshot.ElapsedMs.Should().Be(800);
        snapshot.Children.Should().HaveCount(1);

        var ctsPump = snapshot.Children![0];
        ctsPump.Name.Should().Be("CTS Pump");
        ctsPump.ElapsedMs.Should().Be(800);
        ctsPump.Children.Should().HaveCount(2);

        var fetching = ctsPump.Children!.Single(c => c.Name == "fetching");
        fetching.ElapsedMs.Should().Be(300);

        var processing = ctsPump.Children!.Single(c => c.Name == "record_processing");
        processing.ElapsedMs.Should().Be(500);
    }

    [Fact]
    public void Track_MultipleTopLevelPaths_CreatesMultipleChildren()
    {
        var tree = new TimingTree();

        tree.Track("CTS Pump/fetching", 100);
        tree.Track("SAM Pump/fetching", 200);
        tree.Track("CPH-LID Lookup/fetching", 300);

        var snapshot = tree.Snapshot();
        snapshot.Children.Should().HaveCount(3);
        snapshot.ElapsedMs.Should().Be(600);
    }

    [Fact]
    public void Snapshot_SetsFormattedElapsed()
    {
        var tree = new TimingTree();

        // 1 hour, 2 minutes, 3 seconds, 400 ms
        tree.Track("slow_operation", 3_723_400);

        var snapshot = tree.Snapshot();
        var child = snapshot.Children![0];
        child.Elapsed.Should().Be("01:02:03.4");
    }

    [Fact]
    public void Snapshot_RollsUpParentTotals()
    {
        var tree = new TimingTree();

        tree.Track("Analysis/CTS Pump/fetching", 100);
        tree.Track("Analysis/CTS Pump/processing", 200);
        tree.Track("Analysis/SAM Pump/fetching", 300);

        var snapshot = tree.Snapshot();
        snapshot.ElapsedMs.Should().Be(600);

        var analysis = snapshot.Children!.Single(c => c.Name == "Analysis");
        analysis.ElapsedMs.Should().Be(600);

        var ctsPump = analysis.Children!.Single(c => c.Name == "CTS Pump");
        ctsPump.ElapsedMs.Should().Be(300);

        var samPump = analysis.Children!.Single(c => c.Name == "SAM Pump");
        samPump.ElapsedMs.Should().Be(300);
    }

    [Fact]
    public void Snapshot_EmptyTree_ReturnsRootWithZero()
    {
        var tree = new TimingTree();
        var snapshot = tree.Snapshot();

        snapshot.Name.Should().Be("total");
        snapshot.ElapsedMs.Should().Be(0);
        snapshot.Children.Should().BeNull();
    }

    [Fact]
    public void Snapshot_CustomRootName()
    {
        var tree = new TimingTree();
        tree.Track("a", 1);
        var snapshot = tree.Snapshot("my-root");
        snapshot.Name.Should().Be("my-root");
    }

    [Fact]
    public void Merge_CombinesTwoTrees()
    {
        var main = new TimingTree();
        main.Track("existing", 50);

        var phase = new TimingTree();
        phase.Track("CTS Pump/fetching", 100);
        phase.Track("SAM Pump/fetching", 200);

        main.Merge(phase, "Analysis");

        var snapshot = main.Snapshot();
        snapshot.Children.Should().HaveCount(2); // "existing" + "Analysis"

        var analysis = snapshot.Children!.Single(c => c.Name == "Analysis");
        analysis.ElapsedMs.Should().Be(300);
        analysis.Children.Should().HaveCount(2);
    }

    [Fact]
    public void Merge_EmptyPrefix_MergesAtRoot()
    {
        var main = new TimingTree();
        var phase = new TimingTree();
        phase.Track("fetching", 100);

        main.Merge(phase, "");

        var snapshot = main.Snapshot();
        snapshot.Children![0].Name.Should().Be("fetching");
        snapshot.Children![0].ElapsedMs.Should().Be(100);
    }

    [Fact]
    public void Merge_AccumulatesExistingPaths()
    {
        var main = new TimingTree();
        main.Track("Analysis/CTS Pump/fetching", 100);

        var phase = new TimingTree();
        phase.Track("CTS Pump/fetching", 50);

        main.Merge(phase, "Analysis");

        var snapshot = main.Snapshot();
        var fetching = snapshot.Children!
            .Single(c => c.Name == "Analysis").Children!
            .Single(c => c.Name == "CTS Pump").Children!
            .Single(c => c.Name == "fetching");
        fetching.ElapsedMs.Should().Be(150);
    }

    [Fact]
    public void Snapshot_LeafNodesHaveNullChildren()
    {
        var tree = new TimingTree();
        tree.Track("CTS Pump/fetching", 100);

        var snapshot = tree.Snapshot();
        var leaf = snapshot.Children![0].Children![0];
        leaf.Children.Should().BeNull();
    }

    [Fact]
    public void FormatElapsed_Various()
    {
        TimingNode.FormatElapsed(0).Should().Be("00:00:00.0");
        TimingNode.FormatElapsed(100).Should().Be("00:00:00.1");
        TimingNode.FormatElapsed(61_500).Should().Be("00:01:01.5");
        TimingNode.FormatElapsed(3_600_000).Should().Be("01:00:00.0");
    }
}
