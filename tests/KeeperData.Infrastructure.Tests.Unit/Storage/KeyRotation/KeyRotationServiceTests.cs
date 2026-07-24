using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Locking;
using KeeperData.Core.Storage.KeyRotation;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.KeyRotation;
using KeeperData.Infrastructure.Storage.KeyRotation.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace KeeperData.Infrastructure.Tests.Unit.Storage.KeyRotation;

public class KeyRotationServiceTests
{
    private const string BucketName = "cerespfm-dev-dev1-livestockfeeds";
    private const string ExpectedFileKey = "Dev1_LI_CDP_Int_User_accessKeys.csv";
    private const string NewKeyId = "AKIANEWKEY1234567890";
    private const string NewSecret = "new-secret-value";

    private readonly Mock<IS3ClientFactory> _s3ClientFactoryMock = new();
    private readonly Mock<IAmazonS3> _s3ClientMock = new();
    private readonly Mock<IKeyRotationStore> _storeMock = new();
    private readonly Mock<IS3CredentialValidator> _validatorMock = new();
    private readonly Mock<IDistributedLock> _lockMock = new();

    public KeyRotationServiceTests()
    {
        _s3ClientFactoryMock.Setup(f => f.GetClientBucketName<ExternalStorageClient>()).Returns(BucketName);
        _s3ClientFactoryMock.Setup(f => f.GetClient<ExternalStorageClient>()).Returns(_s3ClientMock.Object);

        _lockMock.Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<IDistributedLockHandle>());

        _validatorMock.Setup(v => v.ValidateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3CredentialValidationResult(S3CredentialValidationOutcome.Valid));

        _storeMock.Setup(s => s.IsEncryptionConfigured).Returns(true);
        _storeMock.Setup(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((ValidatedRotation rotation, CancellationToken _) => new KeyRotationRecord
            {
                Id = Guid.NewGuid().ToString(),
                BucketName = rotation.BucketName,
                Source = rotation.Source,
                Status = KeyRotationStatus.Active,
                FileKey = rotation.FileKey,
                FileHash = rotation.FileHash,
                KeyIdMasked = KeyIdMask.Mask(rotation.AccessKeyId),
                RolledBackFromId = rotation.RolledBackFromId
            });
    }

    private KeyRotationService CreateSut() => new(
        _s3ClientFactoryMock.Object,
        _storeMock.Object,
        _validatorMock.Object,
        _lockMock.Object,
        new ExternalStorageKeyRotationOptions(),
        Mock.Of<ILogger<KeyRotationService>>());

    private void SetupKeyFile(string content)
    {
        _s3ClientMock.Setup(c => c.GetObjectAsync(
                It.Is<GetObjectRequest>(r => r.BucketName == BucketName && r.Key == ExpectedFileKey),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new GetObjectResponse
            {
                BucketName = BucketName,
                Key = ExpectedFileKey,
                ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes(content))
            });
    }

    private static string ValidCsv => $"Access key ID,Secret access key\n{NewKeyId},{NewSecret}\n";

    private static string HashOf(string content) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(content))).ToLowerInvariant();

    [Fact]
    public async Task CheckAndRotate_WhenEncryptionKeyNotConfigured_ReturnsNotConfiguredWithoutLocking()
    {
        // Arrange
        _storeMock.Setup(s => s.IsEncryptionConfigured).Returns(false);
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.NotConfigured);
        result.BucketName.Should().Be(BucketName);
        _lockMock.Verify(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CheckAndRotate_WhenLockUnavailable_ReturnsLockUnavailable()
    {
        // Arrange
        _lockMock.Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((IDistributedLockHandle?)null);
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.LockUnavailable);
    }

    [Fact]
    public async Task CheckAndRotate_WhenFileAbsent_ReturnsFileNotFound()
    {
        // Arrange
        _s3ClientMock.Setup(c => c.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonS3Exception("missing") { StatusCode = HttpStatusCode.NotFound, ErrorCode = "NoSuchKey" });
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.FileNotFound);
        result.FileKey.Should().Be(ExpectedFileKey);
        _storeMock.Verify(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CheckAndRotate_WhenDownloadFailsTransiently_ReturnsTransientError()
    {
        // Arrange
        _s3ClientMock.Setup(c => c.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonS3Exception("boom") { StatusCode = HttpStatusCode.InternalServerError });
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.TransientError);
        _storeMock.Verify(s => s.RecordFailedAsync(It.IsAny<FailedRotation>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CheckAndRotate_WhenHashAlreadyProcessed_ReturnsAlreadyProcessed()
    {
        // Arrange
        SetupKeyFile(ValidCsv);
        _storeMock.Setup(s => s.GetLatestObservedFileHashAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(HashOf(ValidCsv));
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.AlreadyProcessed);
        result.FileHash.Should().Be(HashOf(ValidCsv));
        _validatorMock.Verify(v => v.ValidateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        _storeMock.Verify(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CheckAndRotate_WithNewValidFile_AdoptsCredentials()
    {
        // Arrange
        SetupKeyFile(ValidCsv);
        _storeMock.Setup(s => s.GetLatestObservedFileHashAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((string?)null);
        ValidatedRotation? activated = null;
        _storeMock.Setup(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()))
            .Callback<ValidatedRotation, CancellationToken>((r, _) => activated = r)
            .ReturnsAsync((ValidatedRotation rotation, CancellationToken _) => new KeyRotationRecord
            {
                Id = "new-id",
                BucketName = rotation.BucketName,
                Source = rotation.Source,
                Status = KeyRotationStatus.Active,
                KeyIdMasked = KeyIdMask.Mask(rotation.AccessKeyId)
            });
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.Adopted);
        result.KeyIdHint.Should().Be("AKI...890");
        result.FileHash.Should().Be(HashOf(ValidCsv));

        activated.Should().NotBeNull();
        activated!.Source.Should().Be(KeyRotationSource.Automatic);
        activated.BucketName.Should().Be(BucketName);
        activated.FileKey.Should().Be(ExpectedFileKey);
        activated.FileHash.Should().Be(HashOf(ValidCsv));
        activated.AccessKeyId.Should().Be(NewKeyId);
        activated.SecretAccessKey.Should().Be(NewSecret);
    }

    [Fact]
    public async Task CheckAndRotate_WithUnparseableFile_RecordsFailedAndReturnsInvalidFileFormat()
    {
        // Arrange
        const string garbage = "this is not a csv";
        SetupKeyFile(garbage);
        _storeMock.Setup(s => s.GetLatestObservedFileHashAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((string?)null);
        FailedRotation? failed = null;
        _storeMock.Setup(s => s.RecordFailedAsync(It.IsAny<FailedRotation>(), It.IsAny<CancellationToken>()))
            .Callback<FailedRotation, CancellationToken>((r, _) => failed = r)
            .Returns(Task.CompletedTask);
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.InvalidFileFormat);
        failed.Should().NotBeNull();
        failed!.FileHash.Should().Be(HashOf(garbage));
        failed.FailureReason.Should().NotBeNullOrEmpty();
        _storeMock.Verify(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CheckAndRotate_WhenCredentialsRejected_RecordsFailedAndReturnsValidationFailed()
    {
        // Arrange
        SetupKeyFile(ValidCsv);
        _storeMock.Setup(s => s.GetLatestObservedFileHashAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((string?)null);
        _validatorMock.Setup(v => v.ValidateAsync(NewKeyId, NewSecret, BucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3CredentialValidationResult(S3CredentialValidationOutcome.InvalidCredentials, "S3 rejected the credentials: InvalidAccessKeyId (HTTP 403)"));
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.ValidationFailed);
        result.KeyIdHint.Should().Be("AKI...890");
        _storeMock.Verify(s => s.RecordFailedAsync(
            It.Is<FailedRotation>(rec => rec.FileHash == HashOf(ValidCsv) && rec.KeyIdMasked == "AKI...890"),
            It.IsAny<CancellationToken>()), Times.Once);
        _storeMock.Verify(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CheckAndRotate_WhenValidationTransient_ReturnsTransientErrorWithoutRecording()
    {
        // Arrange
        SetupKeyFile(ValidCsv);
        _storeMock.Setup(s => s.GetLatestObservedFileHashAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((string?)null);
        _validatorMock.Setup(v => v.ValidateAsync(NewKeyId, NewSecret, BucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3CredentialValidationResult(S3CredentialValidationOutcome.TransientError, "Probe failed: HttpRequestException"));
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.TransientError);
        _storeMock.Verify(s => s.RecordFailedAsync(It.IsAny<FailedRotation>(), It.IsAny<CancellationToken>()), Times.Never);
        _storeMock.Verify(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ApplyManual_WithValidCredentials_ActivatesManualRotation()
    {
        // Arrange
        ValidatedRotation? activated = null;
        _storeMock.Setup(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()))
            .Callback<ValidatedRotation, CancellationToken>((r, _) => activated = r)
            .ReturnsAsync((ValidatedRotation rotation, CancellationToken _) => new KeyRotationRecord
            {
                Id = "manual-id",
                BucketName = rotation.BucketName,
                Source = rotation.Source,
                Status = KeyRotationStatus.Active,
                KeyIdMasked = KeyIdMask.Mask(rotation.AccessKeyId)
            });
        var sut = CreateSut();

        // Act
        var result = await sut.ApplyManualAsync(NewKeyId, NewSecret);

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.Applied);
        result.Record.Should().NotBeNull();
        activated!.Source.Should().Be(KeyRotationSource.Manual);
        activated.FileKey.Should().BeNull();
        activated.FileHash.Should().BeNull();
    }

    [Fact]
    public async Task ApplyManual_WhenNotConfigured_ReturnsNotConfigured()
    {
        // Arrange
        _storeMock.Setup(s => s.IsEncryptionConfigured).Returns(false);
        var sut = CreateSut();

        // Act
        var result = await sut.ApplyManualAsync(NewKeyId, NewSecret);

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.NotConfigured);
    }

    [Fact]
    public async Task ApplyManual_WithMissingValues_ReturnsInvalidRequest()
    {
        // Arrange
        var sut = CreateSut();

        // Act
        var result = await sut.ApplyManualAsync("", NewSecret);

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.InvalidRequest);
    }

    [Fact]
    public async Task ApplyManual_WhenCredentialsRejected_ReturnsValidationFailedWithoutStoring()
    {
        // Arrange
        _validatorMock.Setup(v => v.ValidateAsync(NewKeyId, NewSecret, BucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3CredentialValidationResult(S3CredentialValidationOutcome.InvalidCredentials, "nope"));
        var sut = CreateSut();

        // Act
        var result = await sut.ApplyManualAsync(NewKeyId, NewSecret);

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.ValidationFailed);
        _storeMock.Verify(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Rollback_WithUnknownId_ReturnsNotFound()
    {
        // Arrange
        _storeMock.Setup(s => s.GetByIdAsync("missing", It.IsAny<CancellationToken>()))
            .ReturnsAsync((KeyRotationRecord?)null);
        var sut = CreateSut();

        // Act
        var result = await sut.RollbackAsync("missing");

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.NotFound);
    }

    [Fact]
    public async Task Rollback_ToFailedRecord_ReturnsInvalidRequest()
    {
        // Arrange
        _storeMock.Setup(s => s.GetByIdAsync("failed-id", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KeyRotationRecord
            {
                Id = "failed-id",
                BucketName = BucketName,
                Status = KeyRotationStatus.Failed,
                KeyIdMasked = "AKI...890"
            });
        var sut = CreateSut();

        // Act
        var result = await sut.RollbackAsync("failed-id");

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.InvalidRequest);
    }

    [Fact]
    public async Task Rollback_ToActiveRecord_ReturnsInvalidRequest()
    {
        // Arrange
        _storeMock.Setup(s => s.GetByIdAsync("active-id", It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateStoredRecord("active-id", KeyRotationStatus.Active));
        var sut = CreateSut();

        // Act
        var result = await sut.RollbackAsync("active-id");

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.InvalidRequest);
        result.Detail.Should().Contain("already active");
    }

    [Fact]
    public async Task Rollback_ToSupersededRecord_RevalidatesAndActivates()
    {
        // Arrange
        var target = CreateStoredRecord("old-id", KeyRotationStatus.Superseded);
        _storeMock.Setup(s => s.GetByIdAsync("old-id", It.IsAny<CancellationToken>()))
            .ReturnsAsync(target);
        _storeMock.Setup(s => s.DecryptCredentials(target)).Returns((NewKeyId, NewSecret));
        ValidatedRotation? activated = null;
        _storeMock.Setup(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()))
            .Callback<ValidatedRotation, CancellationToken>((r, _) => activated = r)
            .ReturnsAsync((ValidatedRotation rotation, CancellationToken _) => new KeyRotationRecord
            {
                Id = "rollback-id",
                BucketName = rotation.BucketName,
                Source = rotation.Source,
                Status = KeyRotationStatus.Active,
                KeyIdMasked = KeyIdMask.Mask(rotation.AccessKeyId),
                RolledBackFromId = rotation.RolledBackFromId
            });
        var sut = CreateSut();

        // Act
        var result = await sut.RollbackAsync("old-id");

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.Applied);
        activated!.Source.Should().Be(KeyRotationSource.Rollback);
        activated.RolledBackFromId.Should().Be("old-id");
        _validatorMock.Verify(v => v.ValidateAsync(NewKeyId, NewSecret, BucketName, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Rollback_WhenOldCredentialsNoLongerValid_ReturnsValidationFailed()
    {
        // Arrange
        var target = CreateStoredRecord("old-id", KeyRotationStatus.Superseded);
        _storeMock.Setup(s => s.GetByIdAsync("old-id", It.IsAny<CancellationToken>()))
            .ReturnsAsync(target);
        _storeMock.Setup(s => s.DecryptCredentials(target)).Returns((NewKeyId, NewSecret));
        _validatorMock.Setup(v => v.ValidateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3CredentialValidationResult(S3CredentialValidationOutcome.InvalidCredentials, "revoked"));
        var sut = CreateSut();

        // Act
        var result = await sut.RollbackAsync("old-id");

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.ValidationFailed);
        _storeMock.Verify(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ApplyManual_WhenLockUnavailable_ReturnsLockUnavailable()
    {
        // Arrange
        _lockMock.Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((IDistributedLockHandle?)null);
        var sut = CreateSut();

        // Act
        var result = await sut.ApplyManualAsync(NewKeyId, NewSecret);

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.LockUnavailable);
    }

    [Fact]
    public async Task ApplyManual_WhenValidationTransient_ReturnsTransientError()
    {
        // Arrange
        _validatorMock.Setup(v => v.ValidateAsync(NewKeyId, NewSecret, BucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3CredentialValidationResult(S3CredentialValidationOutcome.TransientError, "timeout"));
        var sut = CreateSut();

        // Act
        var result = await sut.ApplyManualAsync(NewKeyId, NewSecret);

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.TransientError);
        _storeMock.Verify(s => s.ActivateValidatedAsync(It.IsAny<ValidatedRotation>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Rollback_WhenNotConfigured_ReturnsNotConfigured()
    {
        // Arrange
        _storeMock.Setup(s => s.IsEncryptionConfigured).Returns(false);
        var sut = CreateSut();

        // Act
        var result = await sut.RollbackAsync("any-id");

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.NotConfigured);
    }

    [Fact]
    public async Task Rollback_WithBlankId_Throws()
    {
        // Arrange
        var sut = CreateSut();

        // Act
        var act = () => sut.RollbackAsync(" ");

        // Assert
        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task Rollback_WhenLockUnavailable_ReturnsLockUnavailable()
    {
        // Arrange
        _lockMock.Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((IDistributedLockHandle?)null);
        var sut = CreateSut();

        // Act
        var result = await sut.RollbackAsync("old-id");

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.LockUnavailable);
    }

    private static KeyRotationRecord CreateStoredRecord(string id, KeyRotationStatus status) => new()
    {
        Id = id,
        BucketName = BucketName,
        RotatedAtUtc = DateTime.UtcNow,
        Source = KeyRotationSource.Automatic,
        Status = status,
        KeyIdMasked = KeyIdMask.Mask(NewKeyId),
        EncryptedAccessKeyId = new EncryptedSecret { KeyVersion = 1, Nonce = "n", CipherText = "c", Tag = "t" },
        EncryptedSecretAccessKey = new EncryptedSecret { KeyVersion = 1, Nonce = "n2", CipherText = "c2", Tag = "t2" }
    };
}
