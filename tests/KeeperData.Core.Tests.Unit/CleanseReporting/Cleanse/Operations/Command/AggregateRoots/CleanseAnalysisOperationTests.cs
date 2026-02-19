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

        op.UpdateProgress(75.0, "Processing", 150, 8, 1);

        op.ProgressPercentage.Should().Be(75.0);
        op.StatusDescription.Should().Be("Processing");
        op.RecordsAnalyzed.Should().Be(150);
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
}
