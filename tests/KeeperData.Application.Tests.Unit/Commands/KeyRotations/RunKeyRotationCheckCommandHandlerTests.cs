using FluentAssertions;
using KeeperData.Application.Commands.KeyRotations;
using KeeperData.Core.Storage.KeyRotation;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Application.Tests.Unit.Commands.KeyRotations;

public class RunKeyRotationCheckCommandHandlerTests
{
    private readonly Mock<IKeyRotationService> _serviceMock = new();
    private readonly RunKeyRotationCheckCommandHandler _sut;

    public RunKeyRotationCheckCommandHandlerTests()
    {
        _sut = new RunKeyRotationCheckCommandHandler(
            _serviceMock.Object,
            TimeProvider.System,
            Mock.Of<ILogger<RunKeyRotationCheckCommandHandler>>());
    }

    [Fact]
    public async Task Handle_RunsCheckAndMapsResult()
    {
        // Arrange
        _serviceMock.Setup(s => s.CheckAndRotateAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KeyRotationCheckResult(
                KeyRotationCheckOutcome.Adopted,
                "cerespfm-dev-dev1-livestockfeeds",
                "Dev1_LI_CDP_Int_User_accessKeys.csv",
                "abc123",
                "AKI...890"));

        // Act
        var response = await _sut.Handle(new RunKeyRotationCheckCommand(), CancellationToken.None);

        // Assert
        response.Outcome.Should().Be(KeyRotationCheckOutcome.Adopted);
        response.BucketName.Should().Be("cerespfm-dev-dev1-livestockfeeds");
        response.FileKey.Should().Be("Dev1_LI_CDP_Int_User_accessKeys.csv");
        response.FileHash.Should().Be("abc123");
        response.KeyIdHint.Should().Be("AKI...890");
        response.CheckedAtUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(10));
        _serviceMock.Verify(s => s.CheckAndRotateAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Handle_WithNoOpOutcome_MapsDetail()
    {
        // Arrange
        _serviceMock.Setup(s => s.CheckAndRotateAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KeyRotationCheckResult(
                KeyRotationCheckOutcome.NotConfigured,
                "bucket",
                Detail: "Encryption key 'KEY_ROTATION_ENCRYPTION_KEY' is not configured"));

        // Act
        var response = await _sut.Handle(new RunKeyRotationCheckCommand(), CancellationToken.None);

        // Assert
        response.Outcome.Should().Be(KeyRotationCheckOutcome.NotConfigured);
        response.Detail.Should().Contain("not configured");
        response.FileKey.Should().BeNull();
        response.KeyIdHint.Should().BeNull();
    }

    [Fact]
    public void Response_NeverExposesKeyMaterial()
    {
        // Assert - compile-time shape check: only masked/derived fields exist.
        typeof(KeyRotationCheckResponse).GetProperties()
            .Select(p => p.Name)
            .Should().BeEquivalentTo(
                "Outcome", "BucketName", "FileKey", "FileHash", "KeyIdHint", "Detail", "CheckedAtUtc");
    }
}
