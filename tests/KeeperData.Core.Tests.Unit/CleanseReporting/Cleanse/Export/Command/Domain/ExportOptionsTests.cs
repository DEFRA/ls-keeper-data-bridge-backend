using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Export.Command.Domain;

public class ExportOptionsTests
{
    [Fact]
    public void Default_ShouldHaveNullSinceAndSendNotificationTrue()
    {
        var options = new ExportOptions();

        options.Since.Should().BeNull();
        options.SendNotification.Should().BeTrue();
    }

    [Fact]
    public void WithSince_ShouldSetSinceValue()
    {
        var since = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        var options = new ExportOptions { Since = since };

        options.Since.Should().Be(since);
        options.SendNotification.Should().BeTrue();
    }

    [Fact]
    public void WithSendNotificationFalse_ShouldOverrideDefault()
    {
        var options = new ExportOptions { SendNotification = false };

        options.SendNotification.Should().BeFalse();
        options.Since.Should().BeNull();
    }

    [Fact]
    public void FullySpecified_ShouldSetAllValues()
    {
        var since = new DateTime(2024, 6, 1, 0, 0, 0, DateTimeKind.Utc);

        var options = new ExportOptions { Since = since, SendNotification = false };

        options.Since.Should().Be(since);
        options.SendNotification.Should().BeFalse();
    }
}
