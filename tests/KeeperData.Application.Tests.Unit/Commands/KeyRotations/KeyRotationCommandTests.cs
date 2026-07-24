using FluentAssertions;
using FluentValidation.TestHelper;
using KeeperData.Application.Commands.KeyRotations;
using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Storage.KeyRotation;
using Moq;

namespace KeeperData.Application.Tests.Unit.Commands.KeyRotations;

public class ApplyManualKeyCommandHandlerTests
{
    private readonly Mock<IKeyRotationService> _serviceMock = new();

    [Fact]
    public async Task Handle_DelegatesToServiceAndMapsToSafeResponse()
    {
        // Arrange
        var record = new KeyRotationRecord
        {
            Id = "manual-1",
            BucketName = "bucket",
            RotatedAtUtc = DateTime.UtcNow,
            Source = KeyRotationSource.Manual,
            Status = KeyRotationStatus.Active,
            KeyIdMasked = "AKI...DEF",
            EncryptedAccessKeyId = new EncryptedSecret { KeyVersion = 1, Nonce = "n", CipherText = "c", Tag = "t" },
            EncryptedSecretAccessKey = new EncryptedSecret { KeyVersion = 1, Nonce = "n", CipherText = "c", Tag = "t" }
        };
        _serviceMock.Setup(s => s.ApplyManualAsync("AKIA1234567890ABCDEF", "secret", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KeyRotationActionResult(KeyRotationActionOutcome.Applied, record));
        var sut = new ApplyManualKeyCommandHandler(_serviceMock.Object);

        // Act
        var response = await sut.Handle(new ApplyManualKeyCommand("AKIA1234567890ABCDEF", "secret"), CancellationToken.None);

        // Assert
        response.Outcome.Should().Be(KeyRotationActionOutcome.Applied);
        response.Rotation.Should().NotBeNull();
        response.Rotation!.KeyIdHint.Should().Be("AKI...DEF");
    }

    [Fact]
    public async Task Handle_WithFailureAndNoRecord_MapsNullRotation()
    {
        // Arrange
        _serviceMock.Setup(s => s.ApplyManualAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KeyRotationActionResult(KeyRotationActionOutcome.ValidationFailed, Detail: "rejected"));
        var sut = new ApplyManualKeyCommandHandler(_serviceMock.Object);

        // Act
        var response = await sut.Handle(new ApplyManualKeyCommand("key", "secret"), CancellationToken.None);

        // Assert
        response.Outcome.Should().Be(KeyRotationActionOutcome.ValidationFailed);
        response.Rotation.Should().BeNull();
        response.Detail.Should().Be("rejected");
    }
}

public class ApplyManualKeyCommandValidatorTests
{
    private readonly ApplyManualKeyCommandValidator _validator = new();

    [Fact]
    public void Validate_WithBothValues_Passes()
    {
        // Act
        var result = _validator.TestValidate(new ApplyManualKeyCommand("AKIA1234567890ABCDEF", "secret"));

        // Assert
        result.ShouldNotHaveAnyValidationErrors();
    }

    [Theory]
    [InlineData("", "secret")]
    [InlineData(null, "secret")]
    public void Validate_WithMissingAccessKeyId_Fails(string? accessKeyId, string secret)
    {
        // Act
        var result = _validator.TestValidate(new ApplyManualKeyCommand(accessKeyId!, secret));

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.AccessKeyId);
    }

    [Theory]
    [InlineData("AKIA1234567890ABCDEF", "")]
    [InlineData("AKIA1234567890ABCDEF", null)]
    public void Validate_WithMissingSecret_Fails(string accessKeyId, string? secret)
    {
        // Act
        var result = _validator.TestValidate(new ApplyManualKeyCommand(accessKeyId, secret!));

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.SecretAccessKey);
    }
}

public class RollbackKeyRotationCommandHandlerTests
{
    [Fact]
    public async Task Handle_DelegatesToService()
    {
        // Arrange
        var serviceMock = new Mock<IKeyRotationService>();
        serviceMock.Setup(s => s.RollbackAsync("rotation-1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KeyRotationActionResult(KeyRotationActionOutcome.NotFound, Detail: "missing"));
        var sut = new RollbackKeyRotationCommandHandler(serviceMock.Object);

        // Act
        var response = await sut.Handle(new RollbackKeyRotationCommand("rotation-1"), CancellationToken.None);

        // Assert
        response.Outcome.Should().Be(KeyRotationActionOutcome.NotFound);
        response.Detail.Should().Be("missing");
    }
}

public class RollbackKeyRotationCommandValidatorTests
{
    private readonly RollbackKeyRotationCommandValidator _validator = new();

    [Fact]
    public void Validate_WithId_Passes()
    {
        // Act
        var result = _validator.TestValidate(new RollbackKeyRotationCommand("some-id"));

        // Assert
        result.ShouldNotHaveAnyValidationErrors();
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void Validate_WithMissingId_Fails(string? id)
    {
        // Act
        var result = _validator.TestValidate(new RollbackKeyRotationCommand(id!));

        // Assert
        result.ShouldHaveValidationErrorFor(x => x.RotationId);
    }
}
