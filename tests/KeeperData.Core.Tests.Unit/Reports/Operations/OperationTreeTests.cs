using FluentAssertions;
using KeeperData.Core.Reports.Operations;
using Microsoft.Extensions.Time.Testing;

namespace KeeperData.Core.Tests.Unit.Reports.Operations;

public class OperationTreeTests
{
    private readonly FakeTimeProvider _timeProvider = new(new DateTimeOffset(2025, 1, 15, 10, 0, 0, TimeSpan.Zero));

    #region Constructor

    [Fact]
    public void Constructor_ShouldCreateRootWithInProgressStatus()
    {
        var tree = new OperationTree(_timeProvider);

        var snapshot = tree.Snapshot();
        snapshot.Name.Should().Be("total");
        snapshot.Status.Should().Be(OperationStatuses.InProgress);
    }

    [Fact]
    public void Constructor_ShouldUseCustomRootName()
    {
        var tree = new OperationTree(_timeProvider, rootName: "operation");

        var snapshot = tree.Snapshot();
        snapshot.Name.Should().Be("operation");
    }

    #endregion

    #region CreateScope

    [Fact]
    public void CreateScope_ShouldAddChildToRoot()
    {
        var tree = new OperationTree(_timeProvider);

        tree.CreateScope("Analysis");

        var snapshot = tree.Snapshot();
        snapshot.Children.Should().HaveCount(1);
        snapshot.Children![0].Name.Should().Be("Analysis");
        snapshot.Children![0].Status.Should().Be(OperationStatuses.NotStarted);
    }

    [Fact]
    public void CreateScope_MultipleScopes_ShouldAddMultipleChildren()
    {
        var tree = new OperationTree(_timeProvider);

        tree.CreateScope("Analysis");
        tree.CreateScope("Deactivation");
        tree.CreateScope("Export");

        var snapshot = tree.Snapshot();
        snapshot.Children.Should().HaveCount(3);
        snapshot.Children![0].Name.Should().Be("Analysis");
        snapshot.Children![1].Name.Should().Be("Deactivation");
        snapshot.Children![2].Name.Should().Be("Export");
    }

    #endregion

    #region Track (backward compat)

    [Fact]
    public void Track_SingleLeaf_CreatesNodeWithCorrectMs()
    {
        var tree = new OperationTree(_timeProvider);

        tree.Track("fetching", 500);

        var snapshot = tree.Snapshot();
        snapshot.Children.Should().HaveCount(1);
        snapshot.Children![0].Name.Should().Be("fetching");
        snapshot.Children![0].ElapsedMs.Should().Be(500);
    }

    [Fact]
    public void Track_NestedPath_CreatesHierarchy()
    {
        var tree = new OperationTree(_timeProvider);

        tree.Track("CTS Pump/fetching", 300);
        tree.Track("CTS Pump/processing", 500);

        var snapshot = tree.Snapshot();
        snapshot.Children.Should().HaveCount(1);

        var ctsPump = snapshot.Children![0];
        ctsPump.Name.Should().Be("CTS Pump");
        ctsPump.Children.Should().HaveCount(2);

        ctsPump.Children!.Single(c => c.Name == "fetching").ElapsedMs.Should().Be(300);
        ctsPump.Children!.Single(c => c.Name == "processing").ElapsedMs.Should().Be(500);
    }

    [Fact]
    public void Track_MultipleCalls_AccumulatesMs()
    {
        var tree = new OperationTree(_timeProvider);

        tree.Track("fetching", 100);
        tree.Track("fetching", 200);
        tree.Track("fetching", 300);

        var snapshot = tree.Snapshot();
        snapshot.Children![0].ElapsedMs.Should().Be(600);
    }

    #endregion

    #region Complete

    [Fact]
    public void Complete_ShouldSetRootToCompleted()
    {
        var tree = new OperationTree(_timeProvider);
        _timeProvider.Advance(TimeSpan.FromMinutes(5));

        tree.Complete();

        var snapshot = tree.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Completed);
        snapshot.ElapsedMs.Should().BeGreaterThan(0);
    }

    [Fact]
    public void Complete_ShouldRecordElapsedTime()
    {
        var tree = new OperationTree(_timeProvider);
        _timeProvider.Advance(TimeSpan.FromSeconds(30));

        tree.Complete();

        var snapshot = tree.Snapshot();
        snapshot.ElapsedMs.Should().Be(30_000);
    }

    #endregion

    #region Cancel

    [Fact]
    public void Cancel_ShouldSetRootToCancelled()
    {
        var tree = new OperationTree(_timeProvider);
        _timeProvider.Advance(TimeSpan.FromMinutes(2));

        tree.Cancel();

        var snapshot = tree.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Cancelled);
        snapshot.ElapsedMs.Should().BeGreaterThan(0);
    }

    [Fact]
    public void Cancel_ShouldRecordElapsedTime()
    {
        var tree = new OperationTree(_timeProvider);
        _timeProvider.Advance(TimeSpan.FromSeconds(45));

        tree.Cancel();

        var snapshot = tree.Snapshot();
        snapshot.ElapsedMs.Should().Be(45_000);
    }

    #endregion

    #region Fail

    [Fact]
    public void Fail_ShouldSetRootToFailed()
    {
        var tree = new OperationTree(_timeProvider);
        _timeProvider.Advance(TimeSpan.FromMinutes(1));

        tree.Fail();

        var snapshot = tree.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Failed);
        snapshot.ElapsedMs.Should().Be(60_000);
    }

    #endregion

    #region Finalize

    [Fact]
    public void Finalize_WithCompleted_ShouldSetStatus()
    {
        var tree = new OperationTree(_timeProvider);
        _timeProvider.Advance(TimeSpan.FromSeconds(10));

        tree.Finalize(OperationStatuses.Completed);

        var snapshot = tree.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Completed);
        snapshot.ElapsedMs.Should().Be(10_000);
    }

    [Fact]
    public void Finalize_WithCancelled_ShouldSetStatus()
    {
        var tree = new OperationTree(_timeProvider);
        _timeProvider.Advance(TimeSpan.FromSeconds(5));

        tree.Finalize(OperationStatuses.Cancelled);

        var snapshot = tree.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Cancelled);
    }

    [Fact]
    public void Finalize_WithFailed_ShouldSetStatus()
    {
        var tree = new OperationTree(_timeProvider);
        _timeProvider.Advance(TimeSpan.FromSeconds(3));

        tree.Finalize(OperationStatuses.Failed);

        var snapshot = tree.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Failed);
    }

    [Fact]
    public void Finalize_CalledTwice_ShouldNotAccumulateElapsed()
    {
        var tree = new OperationTree(_timeProvider);
        _timeProvider.Advance(TimeSpan.FromSeconds(10));

        tree.Finalize(OperationStatuses.Completed);
        _timeProvider.Advance(TimeSpan.FromSeconds(5));
        tree.Finalize(OperationStatuses.Failed);

        var snapshot = tree.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Failed);
        // StartedAtUtc is cleared on first Finalize, so second call doesn't add elapsed
        snapshot.ElapsedMs.Should().Be(10_000);
    }

    #endregion

    #region Snapshot

    [Fact]
    public void Snapshot_EmptyTree_ShouldReturnRootOnly()
    {
        var tree = new OperationTree(_timeProvider);

        var snapshot = tree.Snapshot();

        snapshot.Name.Should().Be("total");
        snapshot.Children.Should().BeNull();
    }

    [Fact]
    public void Snapshot_ShouldFormatElapsed()
    {
        var tree = new OperationTree(_timeProvider);
        tree.Track("slow", 3_723_400); // 1h 2m 3.4s

        var snapshot = tree.Snapshot();
        snapshot.Children![0].Elapsed.Should().Be("01:02:03.4");
    }

    [Fact]
    public void Snapshot_ShouldRollUpElapsedFromChildren()
    {
        var tree = new OperationTree(_timeProvider);
        tree.Track("a", 100);
        tree.Track("b", 200);

        var snapshot = tree.Snapshot();
        snapshot.ElapsedMs.Should().Be(300);
    }

    [Fact]
    public void Snapshot_InProgressRoot_ShouldIncludeLiveElapsed()
    {
        var tree = new OperationTree(_timeProvider);
        _timeProvider.Advance(TimeSpan.FromSeconds(15));

        var snapshot = tree.Snapshot();

        snapshot.Status.Should().Be(OperationStatuses.InProgress);
        snapshot.ElapsedMs.Should().Be(15_000);
    }

    [Fact]
    public void Snapshot_WithScopeProgress_ShouldRollUpProcessedAndTotal()
    {
        var tree = new OperationTree(_timeProvider);
        var scope = tree.CreateScope("Phase1");
        scope.Start(totalRecords: 100);
        scope.UpdateProgress(50);

        var snapshot = tree.Snapshot();

        snapshot.ProcessedCount.Should().Be(50);
        snapshot.TotalRecords.Should().Be(100);
        snapshot.PercentComplete.Should().Be(50);
    }

    [Fact]
    public void Snapshot_WithMultipleScopeProgress_ShouldAggregateAcrossChildren()
    {
        var tree = new OperationTree(_timeProvider);

        var scope1 = tree.CreateScope("Phase1");
        scope1.Start(totalRecords: 200);
        scope1.UpdateProgress(100);

        var scope2 = tree.CreateScope("Phase2");
        scope2.Start(totalRecords: 300);
        scope2.UpdateProgress(150);

        var snapshot = tree.Snapshot();

        snapshot.ProcessedCount.Should().Be(250);
        snapshot.TotalRecords.Should().Be(500);
        snapshot.PercentComplete.Should().Be(50);
    }

    [Fact]
    public void Snapshot_CompletedScope_ShouldShowHundredPercent()
    {
        var tree = new OperationTree(_timeProvider);
        var scope = tree.CreateScope("Phase1");
        scope.Start(totalRecords: 100);
        scope.UpdateProgress(100);
        scope.Complete();

        var snapshot = tree.Snapshot();
        var child = snapshot.Children![0];

        child.Status.Should().Be(OperationStatuses.Completed);
        child.PercentComplete.Should().Be(100);
    }

    [Fact]
    public void Snapshot_ScopeWithChildScopes_ShouldBuildHierarchy()
    {
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("Analysis");
        var child = parent.CreateChild("Preload");
        child.Start(totalRecords: 50);
        child.UpdateProgress(25);

        var snapshot = tree.Snapshot();
        var analysisNode = snapshot.Children![0];

        analysisNode.Children.Should().HaveCount(1);
        analysisNode.Children![0].Name.Should().Be("Preload");
        analysisNode.Children![0].ProcessedCount.Should().Be(25);
        analysisNode.Children![0].TotalRecords.Should().Be(50);
    }

    #endregion
}
