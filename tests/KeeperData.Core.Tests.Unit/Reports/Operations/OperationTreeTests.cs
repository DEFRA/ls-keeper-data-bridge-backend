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

    [Fact]
    public void Snapshot_ParentWithOwnTotal_ShouldPreferOwnTotalOverPartialChildAggregate()
    {
        // Simulates Preload with 6 collection children where only 2 have been started.
        // The parent's own TotalRecords (set via Start) should be used as the denominator,
        // not the partial sum from the 2 started children.
        var tree = new OperationTree(_timeProvider);
        var preload = tree.CreateScope("Preload");
        preload.Start(totalRecords: 1000, description: "Loading 1,000 records from 6 collections");

        var child1 = preload.CreateChild("collection_a");
        child1.Start(totalRecords: 300);
        child1.UpdateProgress(60); // 20% of 300

        var child2 = preload.CreateChild("collection_b");
        child2.Start(totalRecords: 200);
        child2.UpdateProgress(40); // 20% of 200

        // 4 children not yet started (no Start() called)
        preload.CreateChild("collection_c");
        preload.CreateChild("collection_d");
        preload.CreateChild("collection_e");
        preload.CreateChild("collection_f");

        var snapshot = tree.Snapshot();
        var preloadNode = snapshot.Children![0];

        // ProcessedCount rolls up from children
        preloadNode.ProcessedCount.Should().Be(100, "60 + 40 from the two started children");

        // TotalRecords should be the parent's own 1000, NOT 500 from children
        preloadNode.TotalRecords.Should().Be(1000, "parent's authoritative total from Start()");

        // Percent should be 100/1000 = 10%, NOT 100/500 = 20%
        preloadNode.PercentComplete.Should().Be(10);
    }

    [Fact]
    public void Snapshot_ParentWithoutOwnTotal_ShouldAggregateFromChildren()
    {
        // When the parent has no own TotalRecords, child aggregation is the only option.
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("Analysis");

        var child1 = parent.CreateChild("Pump1");
        child1.Start(totalRecords: 200);
        child1.UpdateProgress(100);

        var child2 = parent.CreateChild("Pump2");
        child2.Start(totalRecords: 300);
        child2.UpdateProgress(150);

        var snapshot = tree.Snapshot();
        var analysisNode = snapshot.Children![0];

        analysisNode.TotalRecords.Should().Be(500, "aggregated from children");
        analysisNode.ProcessedCount.Should().Be(250);
        analysisNode.PercentComplete.Should().Be(50);
    }

    [Fact]
    public void UpdateTotal_ShouldSetTotalWithoutResettingTimer()
    {
        var tree = new OperationTree(_timeProvider);
        var scope = tree.CreateScope("Analysis");
        scope.Start(description: "Loading reference data...");

        _timeProvider.Advance(TimeSpan.FromSeconds(10));
        scope.UpdateTotal(500, "Analyzing records...");

        _timeProvider.Advance(TimeSpan.FromSeconds(5));
        var snapshot = tree.Snapshot();
        var node = snapshot.Children![0];

        node.TotalRecords.Should().Be(500);
        node.Description.Should().Be("Analyzing records...");
        node.ElapsedMs.Should().Be(15_000, "timer should not have been reset");
    }

    [Fact]
    public void UpdateTotal_WithoutDescription_ShouldPreserveExistingDescription()
    {
        var tree = new OperationTree(_timeProvider);
        var scope = tree.CreateScope("Phase");
        scope.Start(description: "Original");

        scope.UpdateTotal(300);

        var snapshot = tree.Snapshot();
        var node = snapshot.Children![0];

        node.TotalRecords.Should().Be(300);
        node.Description.Should().Be("Original");
    }

    [Fact]
    public void Snapshot_RpmRollup_ShouldSkipCompletedChildren()
    {
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("Analysis");
        parent.Start();

        // Completed child with stale RPM recorded before completion
        var preload = parent.CreateChild("Preload");
        preload.Start(totalRecords: 1000);
        _timeProvider.Advance(TimeSpan.FromSeconds(2));
        preload.UpdateProgress(500);
        _timeProvider.Advance(TimeSpan.FromSeconds(2));
        preload.UpdateProgress(1000);
        preload.Complete();

        // Active child with current RPM
        var pump = parent.CreateChild("Pump");
        pump.Start(totalRecords: 200);
        _timeProvider.Advance(TimeSpan.FromSeconds(2));
        pump.UpdateProgress(50);
        _timeProvider.Advance(TimeSpan.FromSeconds(2));
        pump.UpdateProgress(100);

        var snapshot = tree.Snapshot();
        var parentNode = snapshot.Children![0];
        var pumpNode = parentNode.Children![1];

        // Parent's current RPM should only include the active pump, not completed preload
        parentNode.CurrentRecordsPerMinute.Should().Be(pumpNode.CurrentRecordsPerMinute,
            "completed children's stale RPM should not be included");
    }

    [Fact]
    public void Snapshot_AverageRpmRollup_ShouldComputeFromAggregatedCounts()
    {
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("Analysis");
        parent.Start();

        // Child 1: processed 600 in 60 seconds
        var child1 = parent.CreateChild("Phase1");
        child1.Start(totalRecords: 600);
        _timeProvider.Advance(TimeSpan.FromSeconds(2));
        child1.UpdateProgress(600);
        _timeProvider.Advance(TimeSpan.FromSeconds(58));
        child1.Complete();

        // Child 2: processed 400 in 120 seconds (overlapping with child1)
        var child2 = parent.CreateChild("Phase2");
        child2.Start(totalRecords: 400);
        _timeProvider.Advance(TimeSpan.FromSeconds(2));
        child2.UpdateProgress(200);
        _timeProvider.Advance(TimeSpan.FromSeconds(58));
        child2.UpdateProgress(400);

        var snapshot = tree.Snapshot();
        var parentNode = snapshot.Children![0];

        // Total processed = 1000, max child elapsed = 120s = 2 min
        // Average RPM should be 1000/2 = 500, not sum of individual child averages
        parentNode.AverageRecordsPerMinute.Should().BeGreaterThan(0);
        parentNode.AverageRecordsPerMinute.Should().NotBe(
            (snapshot.Children![0].Children![0].AverageRecordsPerMinute ?? 0)
            + (snapshot.Children![0].Children![1].AverageRecordsPerMinute ?? 0),
            "should compute from aggregated counts, not sum child averages");
    }

    [Fact]
    public void TrackElapsed_ShouldNotMarkChildrenCompleted()
    {
        var tree = new OperationTree(_timeProvider);
        var scope = tree.CreateScope("Pump");
        scope.Start(totalRecords: 100);

        // Simulate multiple batch iterations
        scope.TrackElapsed("fetching", 50);
        scope.TrackElapsed("record_processing", 200);
        scope.TrackElapsed("fetching", 30);
        scope.TrackElapsed("record_processing", 180);
        scope.UpdateProgress(50);

        var snapshot = tree.Snapshot();
        var pumpNode = snapshot.Children![0];
        var fetchNode = pumpNode.Children![0];
        var processNode = pumpNode.Children![1];

        fetchNode.Status.Should().Be(OperationStatuses.NotStarted,
            "timing-only children should not be prematurely completed");
        fetchNode.ElapsedMs.Should().Be(80);
        processNode.Status.Should().Be(OperationStatuses.NotStarted);
        processNode.ElapsedMs.Should().Be(380);
    }

    [Fact]
    public void FinalizeScope_ShouldCascadeStatusToTimingChildren()
    {
        var tree = new OperationTree(_timeProvider);
        var scope = tree.CreateScope("Pump");
        scope.Start(totalRecords: 100);

        scope.TrackElapsed("fetching", 50);
        scope.TrackElapsed("record_processing", 200);
        scope.UpdateProgress(100);
        scope.Complete();

        var snapshot = tree.Snapshot();
        var pumpNode = snapshot.Children![0];

        pumpNode.Children![0].Status.Should().Be(OperationStatuses.Completed,
            "timing children should inherit parent's terminal status");
        pumpNode.Children![1].Status.Should().Be(OperationStatuses.Completed);
    }

    [Fact]
    public void FinalizeScope_ShouldNotCascadeToScopedChildren()
    {
        var tree = new OperationTree(_timeProvider);
        var parent = tree.CreateScope("Analysis");
        parent.Start();

        // Scoped child (has its own Start/TotalRecords) — should NOT be cascaded to
        var child = parent.CreateChild("Pump");
        child.Start(totalRecords: 50);
        child.UpdateProgress(25);

        // Timing child — should be cascaded to
        parent.TrackElapsed("setup", 100);

        parent.Complete();

        var snapshot = tree.Snapshot();
        var analysisNode = snapshot.Children![0];

        // The scoped child keeps its own status (in-progress, not overwritten)
        analysisNode.Children![0].Status.Should().Be(OperationStatuses.InProgress);
        // The timing child gets cascaded
        analysisNode.Children![1].Status.Should().Be(OperationStatuses.Completed);
    }

    [Fact]
    public void FinalizeScope_Failed_ShouldCascadeFailedToTimingChildren()
    {
        var tree = new OperationTree(_timeProvider);
        var scope = tree.CreateScope("Pump");
        scope.Start(totalRecords: 100);
        scope.TrackElapsed("fetching", 50);
        scope.Fail("something broke");

        var snapshot = tree.Snapshot();
        var pumpNode = snapshot.Children![0];

        pumpNode.Children![0].Status.Should().Be(OperationStatuses.Failed,
            "timing children should inherit failed status");
    }

    #endregion
}
