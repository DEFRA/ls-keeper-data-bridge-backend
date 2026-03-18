using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Export.Command.AggregateRoots;
using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Export.Command.AggregateRoots;

public class CleanseExportOperationTests
{
    [Fact]
    public void Create_ShouldReturnPendingOperationWithDefaults()
    {
        var op = CleanseExportOperation.Create();

        op.Id.Should().NotBeNullOrEmpty();
        op.Status.Should().Be(CleanseExportStatus.Pending);
        op.StatusDescription.Should().Contain("pending");
        op.StartedAtUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
        op.ProgressPercentage.Should().Be(0);
        op.TotalRecords.Should().Be(0);
        op.RecordsExported.Should().Be(0);
    }

    [Fact]
    public void Start_ShouldTransitionToRunning()
    {
        var op = CleanseExportOperation.Create();

        op.Start();

        op.Status.Should().Be(CleanseExportStatus.Running);
        op.StatusDescription.Should().Contain("running");
    }

    [Fact]
    public void UpdateProgress_ShouldSetCountersAndPercentage()
    {
        var op = CleanseExportOperation.Create();
        op.Start();

        op.UpdateProgress(50, 200, "Exporting...");

        op.RecordsExported.Should().Be(50);
        op.TotalRecords.Should().Be(200);
        op.ProgressPercentage.Should().Be(25.0);
        op.StatusDescription.Should().Be("Exporting...");
    }

    [Fact]
    public void UpdateProgress_WhenTotalRecordsIsZero_ShouldSetPercentageToZero()
    {
        var op = CleanseExportOperation.Create();

        op.UpdateProgress(0, 0, "No records");

        op.ProgressPercentage.Should().Be(0);
    }

    [Fact]
    public void Complete_ShouldSetCompletedStateAndDuration()
    {
        var op = CleanseExportOperation.Create();
        op.Start();

        op.Complete(5000);

        op.Status.Should().Be(CleanseExportStatus.Completed);
        op.ProgressPercentage.Should().Be(100.0);
        op.StatusDescription.Should().Be("Export completed");
        op.DurationMs.Should().Be(5000);
        op.CompletedAtUtc.Should().NotBeNull();
        op.CompletedAtUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void Fail_ShouldSetFailedStateWithErrorAndDuration()
    {
        var op = CleanseExportOperation.Create();
        op.Start();

        op.Fail("Connection timeout", 3000);

        op.Status.Should().Be(CleanseExportStatus.Failed);
        op.StatusDescription.Should().Be("Export failed");
        op.Error.Should().Be("Connection timeout");
        op.DurationMs.Should().Be(3000);
        op.CompletedAtUtc.Should().NotBeNull();
    }

    [Fact]
    public void SetReportDetails_ShouldSetBothFields()
    {
        var op = CleanseExportOperation.Create();

        op.SetReportDetails("report.zip", "https://download");

        op.ReportObjectKey.Should().Be("report.zip");
        op.ReportUrl.Should().Be("https://download");
    }

    [Fact]
    public void UpdateReportUrl_ShouldOnlyUpdateUrl()
    {
        var op = CleanseExportOperation.Create();
        op.SetReportDetails("report.zip", "https://old");

        op.UpdateReportUrl("https://new");

        op.ReportUrl.Should().Be("https://new");
        op.ReportObjectKey.Should().Be("report.zip");
    }
}
