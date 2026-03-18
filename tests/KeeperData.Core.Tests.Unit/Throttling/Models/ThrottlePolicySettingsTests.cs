using FluentAssertions;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Tests.Unit.Throttling.Models;

public class ThrottlePolicySettingsTests
{
    [Fact]
    public void Default_ShouldHaveExpectedNormalValues()
    {
        var settings = new ThrottlePolicySettings();

        settings.Ingestion.BatchSize.Should().Be(100);
        settings.Ingestion.BatchDelayMs.Should().Be(1000);
        settings.CleanseAnalysis.PumpBatchSize.Should().Be(50);
        settings.CleanseAnalysis.PumpDelayMs.Should().Be(300);
        settings.CleanseExport.StreamBatchSize.Should().Be(500);
        settings.IssueDeactivation.BatchSize.Should().Be(200);
        settings.IssueQuery.StreamBatchSize.Should().Be(500);
    }

    [Fact]
    public void RecordEquality_ShouldWork()
    {
        var a = new ThrottlePolicySettings();
        var b = new ThrottlePolicySettings();

        a.Should().Be(b);
    }

    [Fact]
    public void With_ShouldCreateModifiedCopy()
    {
        var original = new ThrottlePolicySettings();
        var modified = original with { Ingestion = new() { BatchSize = 999 } };

        modified.Ingestion.BatchSize.Should().Be(999);
        original.Ingestion.BatchSize.Should().Be(100);
    }
}
