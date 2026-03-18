using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Operations.Command.AggregateRoots;

public class CleanseAnalysisOperationTests
{
    [Fact]
    public void Create_ShouldReturnRunningOperationWithDefaults()
    {
        var op = CleanseAnalysisOperation.Create(500);

        op.Id.Should().NotBeNullOrEmpty();
        op.Status.Should().Be(CleanseAnalysisStatus.Running);
        op.TotalRecords.Should().Be(500);
        op.StatusDescription.Should().Contain("Initializing");
        op.StartedAtUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void Create_WithDefaultTotalRecords_ShouldBeZero()
    {
        var op = CleanseAnalysisOperation.Create();

        op.TotalRecords.Should().Be(0);
    }

    [Fact]
    public void UpdateProgress_ShouldSetAllFields()
    {
        var op = CleanseAnalysisOperation.Create();

        op.UpdateProgress(75.0, "Processing", 150, 200, 8, 1);

        op.ProgressPercentage.Should().Be(75.0);
        op.StatusDescription.Should().Be("Processing");
        op.RecordsAnalyzed.Should().Be(150);
        op.TotalRecords.Should().Be(200);
        op.IssuesFound.Should().Be(8);
        op.IssuesResolved.Should().Be(1);
    }

    [Fact]
    public void Complete_ShouldSetCompletedStateAndMetrics()
    {
        var op = CleanseAnalysisOperation.Create();

        op.Complete(200, 10, 2, 5000);

        op.Status.Should().Be(CleanseAnalysisStatus.Completed);
        op.ProgressPercentage.Should().Be(100.0);
        op.StatusDescription.Should().Be("Analysis completed");
        op.RecordsAnalyzed.Should().Be(200);
        op.IssuesFound.Should().Be(10);
        op.IssuesResolved.Should().Be(2);
        op.DurationMs.Should().Be(5000);
        op.CompletedAtUtc.Should().NotBeNull();
    }

    [Fact]
    public void Fail_ShouldSetFailedStateAndError()
    {
        var op = CleanseAnalysisOperation.Create();

        op.Fail("Timeout", 3000);

        op.Status.Should().Be(CleanseAnalysisStatus.Failed);
        op.StatusDescription.Should().Be("Analysis failed");
        op.Error.Should().Be("Timeout");
        op.DurationMs.Should().Be(3000);
        op.CompletedAtUtc.Should().NotBeNull();
    }

    [Fact]
    public void SetReportDetails_ShouldSetBothFields()
    {
        var op = CleanseAnalysisOperation.Create();

        op.SetReportDetails("report.zip", "https://download");

        op.ReportObjectKey.Should().Be("report.zip");
        op.ReportUrl.Should().Be("https://download");
    }

    [Fact]
    public void UpdateReportUrl_ShouldOnlyUpdateUrl()
    {
        var op = CleanseAnalysisOperation.Create();
        op.SetReportDetails("report.zip", "https://old");

        op.UpdateReportUrl("https://new");

        op.ReportUrl.Should().Be("https://new");
        op.ReportObjectKey.Should().Be("report.zip");
    }

    [Fact]
    public void RequestCancellation_ShouldSetFlagAndCancellingStatus()
    {
        var op = CleanseAnalysisOperation.Create();

        op.RequestCancellation();

        op.CancellationRequested.Should().BeTrue();
        op.Status.Should().Be(CleanseAnalysisStatus.Cancelling);
        op.StatusDescription.Should().Contain("Cancellation requested");
    }

    [Fact]
    public void Cancel_ShouldSetCancelledStateAndTimestamp()
    {
        var op = CleanseAnalysisOperation.Create();
        op.UpdateProgress(50.0, "Running", 100, 200, 5, 1);

        op.Cancel(30000);

        op.Status.Should().Be(CleanseAnalysisStatus.Cancelled);
        op.StatusDescription.Should().Be("Analysis cancelled by user");
        op.CancelledAtUtc.Should().NotBeNull();
        op.CompletedAtUtc.Should().NotBeNull();
        op.DurationMs.Should().Be(30000);
    }

    [Fact]
    public void Cancel_ShouldCalculateFinalAverageRpm()
    {
        var op = CleanseAnalysisOperation.Create();
        op.UpdateProgress(50.0, "Running", 600, 1200, 5, 1);

        op.Cancel(60000); // 1 minute

        op.FinalAverageRpm.Should().Be(600.0);
    }

    [Fact]
    public void UpdateProgress_ShouldSetTotalRecords()
    {
        var op = CleanseAnalysisOperation.Create();

        op.UpdateProgress(10.0, "Counting", 100, 1000, 0, 0);

        op.TotalRecords.Should().Be(1000);
    }

    #region Phase tracking

    [Fact]
    public void Create_ShouldInitialiseThreeNotStartedPhases()
    {
        var op = CleanseAnalysisOperation.Create();

        op.Phases.Should().HaveCount(3);
        op.Phases.Should().AllSatisfy(p => p.Status.Should().Be("NotStarted"));
        op.Phases.Select(p => p.Name).Should().ContainInOrder("Analysis", "Deactivation", "Export");
    }

    [Fact]
    public void StartPhase_ShouldSetRunningStatusAndCurrentPhase()
    {
        var op = CleanseAnalysisOperation.Create();

        op.StartPhase(OperationPhase.Analysis, 1000);

        var phase = op.Phases.Single(p => p.Name == "Analysis");
        phase.Status.Should().Be("Running");
        phase.TotalRecords.Should().Be(1000);
        phase.StartedAtUtc.Should().NotBeNull();
        op.CurrentPhase.Should().Be("Analysis");
    }

    [Fact]
    public void UpdatePhaseProgress_ShouldUpdateCountersAndPercentage()
    {
        var op = CleanseAnalysisOperation.Create();
        op.StartPhase(OperationPhase.Analysis, 1000);

        op.UpdatePhaseProgress(OperationPhase.Analysis, 500, 1000, "Halfway");

        var phase = op.Phases.Single(p => p.Name == "Analysis");
        phase.RecordsProcessed.Should().Be(500);
        phase.Percentage.Should().Be(50.0);
        phase.Description.Should().Be("Halfway");
        op.StatusDescription.Should().Be("Halfway");
    }

    [Fact]
    public void UpdatePhaseProgress_ShouldRecalculateAggregatePercentage()
    {
        var op = CleanseAnalysisOperation.Create();
        op.StartPhase(OperationPhase.Analysis, 1000);

        op.UpdatePhaseProgress(OperationPhase.Analysis, 500, 1000, "Halfway");

        // Analysis weight = 0.80, 50% of 80 = 40
        op.ProgressPercentage.Should().Be(40.0);
    }

    [Fact]
    public void CompletePhase_ShouldSetCompletedStatusAndDuration()
    {
        var op = CleanseAnalysisOperation.Create();
        op.StartPhase(OperationPhase.Analysis, 1000);

        op.CompletePhase(OperationPhase.Analysis);

        var phase = op.Phases.Single(p => p.Name == "Analysis");
        phase.Status.Should().Be("Completed");
        phase.Percentage.Should().Be(100.0);
        phase.CompletedAtUtc.Should().NotBeNull();
        phase.DurationMs.Should().NotBeNull();
    }

    [Fact]
    public void CompletePhase_ShouldRecalculateAggregate()
    {
        var op = CleanseAnalysisOperation.Create();
        op.StartPhase(OperationPhase.Analysis, 1000);
        op.CompletePhase(OperationPhase.Analysis);

        // Analysis=100% * 0.80 = 80, others=0
        op.ProgressPercentage.Should().Be(80.0);
    }

    [Fact]
    public void CompleteAllPhases_ShouldReach100Percent()
    {
        var op = CleanseAnalysisOperation.Create();

        op.StartPhase(OperationPhase.Analysis, 100);
        op.CompletePhase(OperationPhase.Analysis);
        op.StartPhase(OperationPhase.Deactivation, 50);
        op.CompletePhase(OperationPhase.Deactivation);
        op.StartPhase(OperationPhase.Export, 30);
        op.CompletePhase(OperationPhase.Export);

        op.ProgressPercentage.Should().Be(100.0);
    }

    [Fact]
    public void StartPhase_WithUnknownPhase_ShouldThrow()
    {
        var op = CleanseAnalysisOperation.Create();
        // Remove all phases to simulate a missing phase
        op.Phases.Clear();

        var act = () => op.StartPhase(OperationPhase.Analysis, 100);

        act.Should().Throw<InvalidOperationException>().WithMessage("*not found*");
    }

    [Fact]
    public void UpdatePhaseProgress_WithZeroTotal_ShouldSetZeroPercentage()
    {
        var op = CleanseAnalysisOperation.Create();
        op.StartPhase(OperationPhase.Deactivation, 0);

        op.UpdatePhaseProgress(OperationPhase.Deactivation, 0, 0, "No stale issues");

        var phase = op.Phases.Single(p => p.Name == "Deactivation");
        phase.Percentage.Should().Be(0);
    }

    #endregion
}
