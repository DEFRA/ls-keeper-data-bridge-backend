using FluentAssertions;
using KeeperData.Core.Reports.Operations;
using Microsoft.Extensions.Time.Testing;

namespace KeeperData.Core.Tests.Unit.Reports.Operations;

public class OperationScopeTests
{
    private readonly FakeTimeProvider _timeProvider = new(new DateTimeOffset(2025, 1, 15, 10, 0, 0, TimeSpan.Zero));

    private OperationScope CreateScope(string name = "test", int rpmWindowSeconds = 60)
    {
        var tree = new OperationTree(_timeProvider, rpmWindowSeconds);
        return tree.CreateScope(name);
    }

    #region Start

    [Fact]
    public void Start_ShouldSetStatusToInProgress()
    {
        var scope = CreateScope();

        scope.Start();

        var snapshot = scope.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.InProgress);
    }

    [Fact]
    public void Start_WithTotalRecords_ShouldSetTotalRecords()
    {
        var scope = CreateScope();

        scope.Start(totalRecords: 1000);

        var snapshot = scope.Snapshot();
        snapshot.TotalRecords.Should().Be(1000);
    }

    [Fact]
    public void Start_WithDescription_ShouldSetDescription()
    {
        var scope = CreateScope();

        scope.Start(description: "Loading data...");

        var snapshot = scope.Snapshot();
        snapshot.Description.Should().Be("Loading data...");
    }

    #endregion

    #region UpdateProgress

    [Fact]
    public void UpdateProgress_ShouldSetProcessedCount()
    {
        var scope = CreateScope();
        scope.Start(totalRecords: 100);

        scope.UpdateProgress(50);

        var snapshot = scope.Snapshot();
        snapshot.ProcessedCount.Should().Be(50);
    }

    [Fact]
    public void UpdateProgress_ShouldCalculatePercentage()
    {
        var scope = CreateScope();
        scope.Start(totalRecords: 200);

        scope.UpdateProgress(50);

        var snapshot = scope.Snapshot();
        snapshot.PercentComplete.Should().Be(25.0);
    }

    [Fact]
    public void UpdateProgress_ShouldUpdateDescription()
    {
        var scope = CreateScope();
        scope.Start(totalRecords: 100);

        scope.UpdateProgress(50, "Halfway there");

        var snapshot = scope.Snapshot();
        snapshot.Description.Should().Be("Halfway there");
    }

    [Fact]
    public void UpdateProgress_ShouldNotOverwriteDescriptionWhenNull()
    {
        var scope = CreateScope();
        scope.Start(description: "Initial");

        scope.UpdateProgress(10);

        var snapshot = scope.Snapshot();
        snapshot.Description.Should().Be("Initial");
    }

    #endregion

    #region Rate tracking

    [Fact]
    public void UpdateProgress_ShouldCalculateCurrentRpm()
    {
        var scope = CreateScope();
        scope.Start(totalRecords: 1000);

        // First snapshot at t=0
        scope.UpdateProgress(0);

        // 500 records later at t=1 minute = 500 RPM
        _timeProvider.Advance(TimeSpan.FromMinutes(1));
        scope.UpdateProgress(500);

        var snapshot = scope.Snapshot();
        snapshot.CurrentRecordsPerMinute.Should().Be(500);
    }

    [Fact]
    public void UpdateProgress_ShouldCalculateAverageRpm()
    {
        var scope = CreateScope();
        scope.Start(totalRecords: 1000);

        _timeProvider.Advance(TimeSpan.FromMinutes(2));
        scope.UpdateProgress(600);

        var snapshot = scope.Snapshot();
        // 600 records in 2 minutes = 300 RPM
        snapshot.AverageRecordsPerMinute.Should().Be(300);
    }

    [Fact]
    public void UpdateProgress_ShouldCalculateProjectedEnd()
    {
        var scope = CreateScope();
        scope.Start(totalRecords: 1000);

        scope.UpdateProgress(0);
        _timeProvider.Advance(TimeSpan.FromMinutes(1));
        scope.UpdateProgress(500);

        var snapshot = scope.Snapshot();
        // 500 remaining at 500 RPM = 1 minute remaining = 60000 ms
        snapshot.ProjectedRemainingMs.Should().NotBeNull();
        snapshot.ProjectedRemainingMs!.Value.Should().BeInRange(59_000, 61_000);
        snapshot.ProjectedEndTimeUtc.Should().BeCloseTo(
            _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(1),
            TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void UpdateProgress_NoProjection_WhenAllRecordsProcessed()
    {
        var scope = CreateScope();
        scope.Start(totalRecords: 100);

        scope.UpdateProgress(0);
        _timeProvider.Advance(TimeSpan.FromMinutes(1));
        scope.UpdateProgress(100);

        var snapshot = scope.Snapshot();
        snapshot.ProjectedRemainingMs.Should().BeNull();
        snapshot.ProjectedEndTimeUtc.Should().BeNull();
    }

    #endregion

    #region CreateChild

    [Fact]
    public void CreateChild_ShouldAddChildToScope()
    {
        var scope = CreateScope("parent");

        scope.CreateChild("child1");
        scope.CreateChild("child2");

        var snapshot = scope.Snapshot();
        snapshot.Children.Should().HaveCount(2);
        snapshot.Children![0].Name.Should().Be("child1");
        snapshot.Children![1].Name.Should().Be("child2");
    }

    [Fact]
    public void CreateChild_ShouldReturnWorkingScope()
    {
        var scope = CreateScope("parent");
        var child = scope.CreateChild("child");

        child.Start(totalRecords: 50);
        child.UpdateProgress(25);

        var parentSnapshot = scope.Snapshot();
        parentSnapshot.Children![0].ProcessedCount.Should().Be(25);
        parentSnapshot.Children![0].TotalRecords.Should().Be(50);
    }

    #endregion

    #region TrackElapsed

    [Fact]
    public void TrackElapsed_SingleLevel_ShouldCreateChild()
    {
        var scope = CreateScope();

        scope.TrackElapsed("fetching", 500);

        var snapshot = scope.Snapshot();
        snapshot.Children.Should().HaveCount(1);
        snapshot.Children![0].Name.Should().Be("fetching");
        snapshot.Children![0].ElapsedMs.Should().Be(500);
        snapshot.Children![0].Status.Should().Be(OperationStatuses.Completed);
    }

    [Fact]
    public void TrackElapsed_NestedPath_ShouldCreateHierarchy()
    {
        var scope = CreateScope();

        scope.TrackElapsed("CTS Pump/fetching", 300);
        scope.TrackElapsed("CTS Pump/processing", 200);

        var snapshot = scope.Snapshot();
        snapshot.Children.Should().HaveCount(1);

        var ctsPump = snapshot.Children![0];
        ctsPump.Status.Should().Be(OperationStatuses.NotStarted, "intermediate node should not be marked completed");
        ctsPump.Children.Should().HaveCount(2);
        ctsPump.Children![0].ElapsedMs.Should().Be(300);
        ctsPump.Children![0].Status.Should().Be(OperationStatuses.Completed);
        ctsPump.Children![1].ElapsedMs.Should().Be(200);
        ctsPump.Children![1].Status.Should().Be(OperationStatuses.Completed);
    }

    [Fact]
    public void TrackElapsed_MultipleCalls_ShouldAccumulate()
    {
        var scope = CreateScope();

        scope.TrackElapsed("fetching", 100);
        scope.TrackElapsed("fetching", 200);

        var snapshot = scope.Snapshot();
        snapshot.Children![0].ElapsedMs.Should().Be(300);
        snapshot.Children![0].Status.Should().Be(OperationStatuses.Completed);
    }

    #endregion

    #region Complete

    [Fact]
    public void Complete_ShouldSetStatusToCompleted()
    {
        var scope = CreateScope();
        scope.Start();
        _timeProvider.Advance(TimeSpan.FromSeconds(10));

        scope.Complete();

        var snapshot = scope.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Completed);
        snapshot.ElapsedMs.Should().Be(10_000);
    }

    [Fact]
    public void Complete_ShouldSetProcessedCountToTotal()
    {
        var scope = CreateScope();
        scope.Start(totalRecords: 100);
        scope.UpdateProgress(50);

        scope.Complete();

        var snapshot = scope.Snapshot();
        snapshot.ProcessedCount.Should().Be(100);
    }

    [Fact]
    public void Complete_WithDescription_ShouldSetDescription()
    {
        var scope = CreateScope();
        scope.Start();

        scope.Complete("All done!");

        var snapshot = scope.Snapshot();
        snapshot.Description.Should().Be("All done!");
    }

    [Fact]
    public void Complete_ShouldReturnPercentComplete100()
    {
        var scope = CreateScope();
        scope.Start(totalRecords: 100);

        scope.Complete();

        var snapshot = scope.Snapshot();
        snapshot.PercentComplete.Should().Be(100);
    }

    #endregion

    #region Fail

    [Fact]
    public void Fail_ShouldSetStatusToFailed()
    {
        var scope = CreateScope();
        scope.Start();
        _timeProvider.Advance(TimeSpan.FromSeconds(5));

        scope.Fail("Something went wrong");

        var snapshot = scope.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Failed);
        snapshot.Description.Should().Be("Something went wrong");
        snapshot.ElapsedMs.Should().Be(5_000);
    }

    #endregion

    #region Parent percentage (weighted)

    [Fact]
    public void ParentPercentage_ShouldBeWeightedByTotalRecords()
    {
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("parent");

        // Child 1: 1000 total, 50% done = 500/1000
        var child1 = parent.CreateChild("big");
        child1.Start(totalRecords: 1000);
        child1.UpdateProgress(500);

        // Child 2: 100 total, 100% done = 100/100
        var child2 = parent.CreateChild("small");
        child2.Start(totalRecords: 100);
        child2.Complete();

        // Weighted: (500/1000 * 1000 + 100/100 * 100) / (1000 + 100)
        // = (50 * 1000 + 100 * 100) / 1100
        // = (50000 + 10000) / 1100 = 54.55
        var snapshot = parent.Snapshot();
        snapshot.PercentComplete.Should().BeApproximately(54.55, 0.01);
    }

    [Fact]
    public void ParentPercentage_ChildrenWithoutTotalRecords_ShouldUseWeight1()
    {
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("parent");

        // Child with no totalRecords but completed
        var child1 = parent.CreateChild("step1");
        child1.Start();
        child1.Complete();

        // Child with no totalRecords, not started
        parent.CreateChild("step2");

        // Weight 1 each: (100 * 1 + 0 * 1) / 2 = 50
        var snapshot = parent.Snapshot();
        snapshot.PercentComplete.Should().Be(50);
    }

    #endregion

    #region Elapsed time (rolling)

    [Fact]
    public void Snapshot_InProgressScope_ShouldIncludeLiveElapsed()
    {
        var scope = CreateScope();
        scope.Start();

        _timeProvider.Advance(TimeSpan.FromSeconds(30));

        var snapshot = scope.Snapshot();
        snapshot.ElapsedMs.Should().Be(30_000);
        snapshot.Elapsed.Should().Be("00:00:30.0");
    }

    #endregion

    #region Parent with own TotalRecords and children with TotalRecords (aggregate roll-up)

    [Fact]
    public void ParentWithTotalRecords_ChildrenWithTotalRecords_ShouldPreferChildAggregate()
    {
        // Simulates the Preload scope: parent has TotalRecords = 1100 but ProcessedCount = 0 (stale).
        // Children have live progress updates.
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("preload");
        parent.Start(totalRecords: 1100, description: "Loading 1100 records");

        var child1 = parent.CreateChild("collection_a");
        child1.Start(totalRecords: 1000);
        child1.UpdateProgress(500); // 50% of 1000

        var child2 = parent.CreateChild("collection_b");
        child2.Start(totalRecords: 100);
        child2.Complete(); // 100% of 100

        var snapshot = parent.Snapshot();

        // Should use child-weighted aggregate, NOT parent's own 0/1100 = 0%
        // Weighted: (50*1000 + 100*100) / 1100 = 60000/1100 ≈ 54.55%
        snapshot.PercentComplete.Should().BeApproximately(54.55, 0.01);
    }

    [Fact]
    public void ParentWithTotalRecords_ChildrenWithTotalRecords_ShouldRollUpCounts()
    {
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("preload");
        parent.Start(totalRecords: 300);

        var child1 = parent.CreateChild("col_a");
        child1.Start(totalRecords: 200);
        child1.UpdateProgress(80);

        var child2 = parent.CreateChild("col_b");
        child2.Start(totalRecords: 100);
        child2.UpdateProgress(50);

        var snapshot = parent.Snapshot();

        // Should aggregate from children, not show parent's own ProcessedCount (0)
        snapshot.ProcessedCount.Should().Be(130); // 80 + 50
        snapshot.TotalRecords.Should().Be(300);   // 200 + 100
    }

    [Fact]
    public void ParentWithTotalRecords_ChildrenWithTotalRecords_ShouldRollUpRpm()
    {
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("preload");
        parent.Start(totalRecords: 2000);

        // Child 1 actively processing — simulate windowed RPM
        var child1 = parent.CreateChild("col_a");
        child1.Start(totalRecords: 1000);
        child1.UpdateProgress(100);
        _timeProvider.Advance(TimeSpan.FromMinutes(1));
        child1.UpdateProgress(200); // → ~100 RPM over 1 min window

        // Child 2 actively processing
        var child2 = parent.CreateChild("col_b");
        child2.Start(totalRecords: 1000);
        child2.UpdateProgress(50);
        _timeProvider.Advance(TimeSpan.FromMinutes(1));
        child2.UpdateProgress(100); // → ~50 RPM over 1 min window

        var snapshot = parent.Snapshot();

        // Parent should show summed RPM from children
        snapshot.CurrentRecordsPerMinute.Should().NotBeNull();
        snapshot.CurrentRecordsPerMinute.Should().Be(
            snapshot.Children![0].CurrentRecordsPerMinute!.Value +
            snapshot.Children![1].CurrentRecordsPerMinute!.Value);
    }

    [Fact]
    public void ParentWithTotalRecords_MixedChildren_ShouldNotProjectEndTime()
    {
        // Projections should stay null at the parent level — only leaf nodes project
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("preload");
        parent.Start(totalRecords: 500);

        var child = parent.CreateChild("col_a");
        child.Start(totalRecords: 500);
        child.UpdateProgress(100);
        _timeProvider.Advance(TimeSpan.FromMinutes(1));
        child.UpdateProgress(200);

        var snapshot = parent.Snapshot();

        snapshot.ProjectedRemainingMs.Should().BeNull();
        snapshot.ProjectedEndTimeUtc.Should().BeNull();
    }

    [Fact]
    public void LeafWithTotalRecords_NoChildren_ShouldUseOwnProgress()
    {
        // Regression: a leaf node (like CTS Pump) should still use its own ProcessedCount
        var scope = CreateScope("cts_pump");
        scope.Start(totalRecords: 1000);
        scope.UpdateProgress(250);

        var snapshot = scope.Snapshot();

        snapshot.PercentComplete.Should().Be(25.0);
        snapshot.ProcessedCount.Should().Be(250);
        snapshot.TotalRecords.Should().Be(1000);
    }

    [Fact]
    public void ParentWithTotalRecords_ChildrenNotYetStarted_ShouldShowZeroPercent()
    {
        // All children created but not started — should show 0%
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("preload");
        parent.Start(totalRecords: 500);

        parent.CreateChild("col_a"); // not started, no totalRecords
        parent.CreateChild("col_b"); // not started, no totalRecords

        var snapshot = parent.Snapshot();

        // Children lack TotalRecords → falls through to parent's own 0/500 = 0%
        snapshot.PercentComplete.Should().Be(0);
    }

    #endregion

    #region Cancel

    [Fact]
    public void Cancel_ShouldSetStatusToCancelled()
    {
        var scope = CreateScope();
        scope.Start();
        _timeProvider.Advance(TimeSpan.FromSeconds(7));

        scope.Cancel("User requested cancellation");

        var snapshot = scope.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Cancelled);
        snapshot.Description.Should().Be("User requested cancellation");
        snapshot.ElapsedMs.Should().Be(7_000);
    }

    [Fact]
    public void Cancel_ShouldRecordElapsedTime()
    {
        var scope = CreateScope();
        scope.Start();
        _timeProvider.Advance(TimeSpan.FromSeconds(12));

        scope.Cancel();

        var snapshot = scope.Snapshot();
        snapshot.ElapsedMs.Should().Be(12_000);
    }

    [Fact]
    public void Cancel_WithoutDescription_ShouldNotOverwriteExisting()
    {
        var scope = CreateScope();
        scope.Start(description: "Loading data...");
        _timeProvider.Advance(TimeSpan.FromSeconds(3));

        scope.Cancel();

        var snapshot = scope.Snapshot();
        snapshot.Status.Should().Be(OperationStatuses.Cancelled);
        snapshot.Description.Should().Be("Loading data...");
    }

    #endregion
}
