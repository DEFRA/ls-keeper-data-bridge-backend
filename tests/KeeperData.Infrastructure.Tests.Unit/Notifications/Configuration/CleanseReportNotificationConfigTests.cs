using FluentAssertions;
using KeeperData.Infrastructure.Notifications.Configuration;

namespace KeeperData.Infrastructure.Tests.Unit.Notifications.Configuration;

public class CleanseReportNotificationConfigTests
{
    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var config = new CleanseReportNotificationConfig();

        config.ApiKey.Should().BeEmpty();
        config.TemplateId.Should().NotBeEmpty();
        config.RecipientEmails.Should().NotBeEmpty();
        config.Enabled.Should().BeTrue();
    }

    [Fact]
    public void SectionName_IsCorrect()
    {
        CleanseReportNotificationConfig.SectionName.Should().Be("CleanseReportNotification");
    }

    [Theory]
    [InlineData("", false)]
    [InlineData("   ", false)]
    [InlineData(null, false)]
    [InlineData("test-api-key", true)]
    public void HasApiKey_ReturnsCorrectValue(string? apiKey, bool expected)
    {
        var config = new CleanseReportNotificationConfig { ApiKey = apiKey ?? string.Empty };

        config.HasApiKey.Should().Be(expected);
    }

    [Theory]
    [InlineData(true, "api-key", true)]
    [InlineData(false, "api-key", false)]
    [InlineData(true, "", false)]
    [InlineData(false, "", false)]
    public void IsEnabled_ReturnsCorrectValue(bool enabled, string apiKey, bool expected)
    {
        var config = new CleanseReportNotificationConfig
        {
            Enabled = enabled,
            ApiKey = apiKey
        };

        config.IsEnabled.Should().Be(expected);
    }

    [Fact]
    public void GetRecipientEmailList_WithEmptyString_ReturnsEmptyList()
    {
        var config = new CleanseReportNotificationConfig { RecipientEmails = "" };

        var result = config.GetRecipientEmailList();

        result.Should().BeEmpty();
    }

    [Fact]
    public void GetRecipientEmailList_WithWhitespace_ReturnsEmptyList()
    {
        var config = new CleanseReportNotificationConfig { RecipientEmails = "   " };

        var result = config.GetRecipientEmailList();

        result.Should().BeEmpty();
    }

    [Fact]
    public void GetRecipientEmailList_WithSingleEmail_ReturnsSingleItem()
    {
        var config = new CleanseReportNotificationConfig { RecipientEmails = "test@example.com" };

        var result = config.GetRecipientEmailList();

        result.Should().ContainSingle().Which.Should().Be("test@example.com");
    }

    [Fact]
    public void GetRecipientEmailList_WithCommaDelimitedEmails_SplitsCorrectly()
    {
        var config = new CleanseReportNotificationConfig
        {
            RecipientEmails = "a@test.com, b@test.com, c@test.com"
        };

        var result = config.GetRecipientEmailList();

        result.Should().HaveCount(3);
        result.Should().Contain("a@test.com");
        result.Should().Contain("b@test.com");
        result.Should().Contain("c@test.com");
    }

    [Fact]
    public void GetRecipientEmailList_WithSemicolonDelimitedEmails_SplitsCorrectly()
    {
        var config = new CleanseReportNotificationConfig
        {
            RecipientEmails = "a@test.com; b@test.com; c@test.com"
        };

        var result = config.GetRecipientEmailList();

        result.Should().HaveCount(3);
    }

    [Fact]
    public void GetRecipientEmailList_WithMixedDelimiters_SplitsCorrectly()
    {
        var config = new CleanseReportNotificationConfig
        {
            RecipientEmails = "a@test.com, b@test.com; c@test.com"
        };

        var result = config.GetRecipientEmailList();

        result.Should().HaveCount(3);
    }

    [Fact]
    public void GetRecipientEmailList_RemovesDuplicates()
    {
        var config = new CleanseReportNotificationConfig
        {
            RecipientEmails = "test@example.com, TEST@EXAMPLE.COM, test@example.com"
        };

        var result = config.GetRecipientEmailList();

        result.Should().ContainSingle();
    }

    [Fact]
    public void GetRecipientEmailList_TrimsWhitespace()
    {
        var config = new CleanseReportNotificationConfig
        {
            RecipientEmails = "  test@example.com  ,  another@test.com  "
        };

        var result = config.GetRecipientEmailList();

        result.Should().Contain("test@example.com");
        result.Should().Contain("another@test.com");
    }

    [Fact]
    public void GetRecipientEmailList_SkipsEmptyEntries()
    {
        var config = new CleanseReportNotificationConfig
        {
            RecipientEmails = "test@example.com,,, , another@test.com"
        };

        var result = config.GetRecipientEmailList();

        result.Should().HaveCount(2);
    }

    [Fact]
    public void GetMaskedRecipientEmails_MasksEmailsCorrectly()
    {
        var config = new CleanseReportNotificationConfig
        {
            RecipientEmails = "test@example.com, another@test.com"
        };

        var result = config.GetMaskedRecipientEmails();

        result.Should().HaveCount(2);
        result[0].Should().Be("tes***");
        result[1].Should().Be("ano***");
    }

    [Fact]
    public void GetMaskedRecipientEmails_WithShortEmail_ReturnsMaskedVersion()
    {
        var config = new CleanseReportNotificationConfig { RecipientEmails = "ab" };

        var result = config.GetMaskedRecipientEmails();

        result.Should().ContainSingle().Which.Should().Be("***");
    }

    [Fact]
    public void GetMaskedRecipientEmails_WithEmptyEmails_ReturnsEmptyList()
    {
        var config = new CleanseReportNotificationConfig { RecipientEmails = "" };

        var result = config.GetMaskedRecipientEmails();

        result.Should().BeEmpty();
    }
}
