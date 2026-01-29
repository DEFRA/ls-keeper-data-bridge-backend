using FluentAssertions;
using KeeperData.Infrastructure.Notifications;
using KeeperData.Infrastructure.Notifications.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Notifications;

/// <summary>
/// Unit tests for GovukNotifyCleanseReportNotificationService.
/// These tests focus on the early-exit validation paths that don't require
/// calling the actual NotificationClient (which is a sealed class).
/// </summary>
public class GovukNotifyCleanseReportNotificationServiceTests
{
    private readonly Mock<INotificationClientFactory> _clientFactoryMock;
    private readonly Mock<ILogger<GovukNotifyCleanseReportNotificationService>> _loggerMock;

    public GovukNotifyCleanseReportNotificationServiceTests()
    {
        _clientFactoryMock = new Mock<INotificationClientFactory>();
        _loggerMock = new Mock<ILogger<GovukNotifyCleanseReportNotificationService>>();
    }

    private GovukNotifyCleanseReportNotificationService CreateService(CleanseReportNotificationConfig config)
    {
        var options = Options.Create(config);
        return new GovukNotifyCleanseReportNotificationService(options, _clientFactoryMock.Object, _loggerMock.Object);
    }

    #region SendReportNotificationAsync - Disabled Path Tests

    [Fact]
    public async Task SendReportNotificationAsync_WhenDisabled_ReturnsSuccessWithSkipMessage()
    {
        // Arrange
        var config = new CleanseReportNotificationConfig
        {
            ApiKey = "test-api-key",
            TemplateId = "test-template-id",
            RecipientEmails = "test@example.com",
            Enabled = false
        };
        var service = CreateService(config);

        // Act
        var result = await service.SendReportNotificationAsync("https://example.com/report.zip");

        // Assert
        result.Success.Should().BeTrue();
        result.Error.Should().Be("Notifications disabled");
        _clientFactoryMock.Verify(f => f.Create(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task SendReportNotificationAsync_WhenDisabled_IncludesRecipientInResult()
    {
        // Arrange
        var config = new CleanseReportNotificationConfig
        {
            ApiKey = "test-api-key",
            TemplateId = "test-template-id",
            RecipientEmails = "user1@test.com;user2@test.com",
            Enabled = false
        };
        var service = CreateService(config);

        // Act
        var result = await service.SendReportNotificationAsync("https://example.com/report.zip");

        // Assert
        result.Recipient.Should().Be("user1@test.com; user2@test.com");
    }

    #endregion

    #region SendReportNotificationAsync - No API Key Path Tests

    [Fact]
    public async Task SendReportNotificationAsync_WhenNoApiKey_ReturnsFailure()
    {
        // Arrange
        var config = new CleanseReportNotificationConfig
        {
            ApiKey = "",
            TemplateId = "test-template-id",
            RecipientEmails = "test@example.com",
            Enabled = true
        };
        var service = CreateService(config);

        // Act
        var result = await service.SendReportNotificationAsync("https://example.com/report.zip");

        // Assert
        result.Success.Should().BeFalse();
        result.Error.Should().Be("API key not configured");
        _clientFactoryMock.Verify(f => f.Create(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task SendReportNotificationAsync_WhenApiKeyIsWhitespace_ReturnsFailure()
    {
        // Arrange
        var config = new CleanseReportNotificationConfig
        {
            ApiKey = "   ",
            TemplateId = "test-template-id",
            RecipientEmails = "test@example.com",
            Enabled = true
        };
        var service = CreateService(config);

        // Act
        var result = await service.SendReportNotificationAsync("https://example.com/report.zip");

        // Assert
        result.Success.Should().BeFalse();
        result.Error.Should().Be("API key not configured");
    }

    #endregion

    #region SendReportNotificationAsync - No Recipients Path Tests

    [Fact]
    public async Task SendReportNotificationAsync_WhenNoRecipients_ReturnsFailure()
    {
        // Arrange
        var config = new CleanseReportNotificationConfig
        {
            ApiKey = "test-api-key",
            TemplateId = "test-template-id",
            RecipientEmails = "",
            Enabled = true
        };
        var service = CreateService(config);

        // Act
        var result = await service.SendReportNotificationAsync("https://example.com/report.zip");

        // Assert
        result.Success.Should().BeFalse();
        result.Error.Should().Be("No recipient email addresses configured");
        result.Recipient.Should().BeEmpty();
    }

    [Fact]
    public async Task SendReportNotificationAsync_WhenRecipientsIsWhitespace_ReturnsFailure()
    {
        // Arrange
        var config = new CleanseReportNotificationConfig
        {
            ApiKey = "test-api-key",
            TemplateId = "test-template-id",
            RecipientEmails = "   ",
            Enabled = true
        };
        var service = CreateService(config);

        // Act
        var result = await service.SendReportNotificationAsync("https://example.com/report.zip");

        // Assert
        result.Success.Should().BeFalse();
        result.Error.Should().Be("No recipient email addresses configured");
    }

    #endregion

    #region SendTestNotificationAsync - No API Key Path Tests

    [Fact]
    public async Task SendTestNotificationAsync_WhenNoApiKey_ReturnsFailure()
    {
        // Arrange
        var config = new CleanseReportNotificationConfig
        {
            ApiKey = "",
            TemplateId = "test-template-id",
            RecipientEmails = "test@example.com",
            Enabled = true
        };
        var service = CreateService(config);

        // Act
        var result = await service.SendTestNotificationAsync("test@example.com");

        // Assert
        result.Success.Should().BeFalse();
        result.Error.Should().Be("API key not configured");
        result.Recipient.Should().Be("test@example.com");
    }

    [Fact]
    public async Task SendTestNotificationAsync_WhenApiKeyIsEmpty_DoesNotCallFactory()
    {
        // Arrange
        var config = new CleanseReportNotificationConfig
        {
            ApiKey = "",
            TemplateId = "test-template-id",
            RecipientEmails = "test@example.com",
            Enabled = true
        };
        var service = CreateService(config);

        // Act
        await service.SendTestNotificationAsync("test@example.com");

        // Assert
        _clientFactoryMock.Verify(f => f.Create(It.IsAny<string>()), Times.Never);
    }

    #endregion
}
