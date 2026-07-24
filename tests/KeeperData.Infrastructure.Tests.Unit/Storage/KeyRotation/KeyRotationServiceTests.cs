using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Locking;
using KeeperData.Core.Storage.KeyRotation;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.KeyRotation;
using KeeperData.Infrastructure.Storage.KeyRotation.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
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
    private readonly Mock<IKeyRotationRepository> _repositoryMock = new();
    private readonly Mock<IS3CredentialValidator> _validatorMock = new();
    private readonly Mock<IDistributedLock> _lockMock = new();
    private readonly Mock<IExternalStorageCredentialsProvider> _providerMock = new();
    private readonly FakeTimeProvider _timeProvider = new();
    private readonly AesGcmSecretProtector _protector;

    public KeyRotationServiceTests()
    {
        var key = new byte[32];
        for (var i = 0; i < key.Length; i++) key[i] = (byte)(i + 1);
        _protector = AesGcmSecretProtector.FromKey(key);

        _s3ClientFactoryMock.Setup(f => f.GetClientBucketName<ExternalStorageClient>()).Returns(BucketName);
        _s3ClientFactoryMock.Setup(f => f.GetClient<ExternalStorageClient>()).Returns(_s3ClientMock.Object);

        _lockMock.Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<IDistributedLockHandle>());

        _validatorMock.Setup(v => v.ValidateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3CredentialValidationResult(S3CredentialValidationOutcome.Valid));
    }

    private KeyRotationService CreateSut(ISecretProtector? protector = null) => new(
        _s3ClientFactoryMock.Object,
        _repositoryMock.Object,
        protector ?? _protector,
        _validatorMock.Object,
        _lockMock.Object,
        _providerMock.Object,
        new ExternalStorageKeyRotationOptions(),
        _timeProvider,
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
        var sut = CreateSut(AesGcmSecretProtector.Unconfigured());

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
        _repositoryMock.Verify(r => r.ActivateAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()), Times.Never);
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
        _repositoryMock.Verify(r => r.AddFailedAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CheckAndRotate_WhenHashAlreadyProcessed_ReturnsAlreadyProcessed()
    {
        // Arrange
        SetupKeyFile(ValidCsv);
        _repositoryMock.Setup(r => r.GetLatestObservedFileHashAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(HashOf(ValidCsv));
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.AlreadyProcessed);
        result.FileHash.Should().Be(HashOf(ValidCsv));
        _validatorMock.Verify(v => v.ValidateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        _repositoryMock.Verify(r => r.ActivateAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CheckAndRotate_WithNewValidFile_AdoptsCredentials()
    {
        // Arrange
        SetupKeyFile(ValidCsv);
        _repositoryMock.Setup(r => r.GetLatestObservedFileHashAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((string?)null);
        KeyRotationRecord? activated = null;
        _repositoryMock.Setup(r => r.ActivateAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()))
            .Callback<KeyRotationRecord, CancellationToken>((r, _) => activated = r)
            .Returns(Task.CompletedTask);
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
        activated.KeyIdMasked.Should().Be("AKI...890");
        activated.ValidatedAtUtc.Should().NotBeNull();

        // Stored credentials are encrypted and decrypt back to the file values.
        _protector.Unprotect(activated.EncryptedAccessKeyId!, SecretPurposes.AccessKeyId).Should().Be(NewKeyId);
        _protector.Unprotect(activated.EncryptedSecretAccessKey!, SecretPurposes.SecretAccessKey).Should().Be(NewSecret);

        _providerMock.Verify(p => p.Invalidate(), Times.Once);
    }

    [Fact]
    public async Task CheckAndRotate_WithUnparseableFile_RecordsFailedAndReturnsInvalidFileFormat()
    {
        // Arrange
        const string garbage = "this is not a csv";
        SetupKeyFile(garbage);
        _repositoryMock.Setup(r => r.GetLatestObservedFileHashAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((string?)null);
        KeyRotationRecord? failed = null;
        _repositoryMock.Setup(r => r.AddFailedAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()))
            .Callback<KeyRotationRecord, CancellationToken>((r, _) => failed = r)
            .Returns(Task.CompletedTask);
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.InvalidFileFormat);
        failed.Should().NotBeNull();
        failed!.FileHash.Should().Be(HashOf(garbage));
        failed.EncryptedAccessKeyId.Should().BeNull();
        failed.FailureReason.Should().NotBeNullOrEmpty();
        _repositoryMock.Verify(r => r.ActivateAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()), Times.Never);
        _providerMock.Verify(p => p.Invalidate(), Times.Never);
    }

    [Fact]
    public async Task CheckAndRotate_WhenCredentialsRejected_RecordsFailedAndReturnsValidationFailed()
    {
        // Arrange
        SetupKeyFile(ValidCsv);
        _repositoryMock.Setup(r => r.GetLatestObservedFileHashAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((string?)null);
        _validatorMock.Setup(v => v.ValidateAsync(NewKeyId, NewSecret, BucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3CredentialValidationResult(S3CredentialValidationOutcome.InvalidCredentials, "S3 rejected the credentials: InvalidAccessKeyId (HTTP 403)"));
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.ValidationFailed);
        result.KeyIdHint.Should().Be("AKI...890");
        _repositoryMock.Verify(r => r.AddFailedAsync(
            It.Is<KeyRotationRecord>(rec => rec.FileHash == HashOf(ValidCsv) && rec.EncryptedAccessKeyId == null),
            It.IsAny<CancellationToken>()), Times.Once);
        _repositoryMock.Verify(r => r.ActivateAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CheckAndRotate_WhenValidationTransient_ReturnsTransientErrorWithoutRecording()
    {
        // Arrange
        SetupKeyFile(ValidCsv);
        _repositoryMock.Setup(r => r.GetLatestObservedFileHashAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((string?)null);
        _validatorMock.Setup(v => v.ValidateAsync(NewKeyId, NewSecret, BucketName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3CredentialValidationResult(S3CredentialValidationOutcome.TransientError, "Probe failed: HttpRequestException"));
        var sut = CreateSut();

        // Act
        var result = await sut.CheckAndRotateAsync();

        // Assert
        result.Outcome.Should().Be(KeyRotationCheckOutcome.TransientError);
        _repositoryMock.Verify(r => r.AddFailedAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()), Times.Never);
        _repositoryMock.Verify(r => r.ActivateAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ApplyManual_WithValidCredentials_ActivatesManualRotation()
    {
        // Arrange
        KeyRotationRecord? activated = null;
        _repositoryMock.Setup(r => r.ActivateAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()))
            .Callback<KeyRotationRecord, CancellationToken>((r, _) => activated = r)
            .Returns(Task.CompletedTask);
        var sut = CreateSut();

        // Act
        var result = await sut.ApplyManualAsync(NewKeyId, NewSecret);

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.Applied);
        result.Record.Should().NotBeNull();
        activated!.Source.Should().Be(KeyRotationSource.Manual);
        activated.FileKey.Should().BeNull();
        activated.FileHash.Should().BeNull();
        _providerMock.Verify(p => p.Invalidate(), Times.Once);
    }

    [Fact]
    public async Task ApplyManual_WhenNotConfigured_ReturnsNotConfigured()
    {
        // Arrange
        var sut = CreateSut(AesGcmSecretProtector.Unconfigured());

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
        _repositoryMock.Verify(r => r.ActivateAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Rollback_WithUnknownId_ReturnsNotFound()
    {
        // Arrange
        _repositoryMock.Setup(r => r.GetByIdAsync("missing", It.IsAny<CancellationToken>()))
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
        _repositoryMock.Setup(r => r.GetByIdAsync("failed-id", It.IsAny<CancellationToken>()))
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
        _repositoryMock.Setup(r => r.GetByIdAsync("active-id", It.IsAny<CancellationToken>()))
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
        _repositoryMock.Setup(r => r.GetByIdAsync("old-id", It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateStoredRecord("old-id", KeyRotationStatus.Superseded));
        KeyRotationRecord? activated = null;
        _repositoryMock.Setup(r => r.ActivateAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()))
            .Callback<KeyRotationRecord, CancellationToken>((r, _) => activated = r)
            .Returns(Task.CompletedTask);
        var sut = CreateSut();

        // Act
        var result = await sut.RollbackAsync("old-id");

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.Applied);
        activated!.Source.Should().Be(KeyRotationSource.Rollback);
        activated.RolledBackFromId.Should().Be("old-id");
        _validatorMock.Verify(v => v.ValidateAsync(NewKeyId, NewSecret, BucketName, It.IsAny<CancellationToken>()), Times.Once);
        _providerMock.Verify(p => p.Invalidate(), Times.Once);
    }

    [Fact]
    public async Task Rollback_WhenOldCredentialsNoLongerValid_ReturnsValidationFailed()
    {
        // Arrange
        _repositoryMock.Setup(r => r.GetByIdAsync("old-id", It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateStoredRecord("old-id", KeyRotationStatus.Superseded));
        _validatorMock.Setup(v => v.ValidateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new S3CredentialValidationResult(S3CredentialValidationOutcome.InvalidCredentials, "revoked"));
        var sut = CreateSut();

        // Act
        var result = await sut.RollbackAsync("old-id");

        // Assert
        result.Outcome.Should().Be(KeyRotationActionOutcome.ValidationFailed);
        _repositoryMock.Verify(r => r.ActivateAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private KeyRotationRecord CreateStoredRecord(string id, KeyRotationStatus status) => new()
    {
        Id = id,
        BucketName = BucketName,
        RotatedAtUtc = _timeProvider.GetUtcNow().UtcDateTime,
        Source = KeyRotationSource.Automatic,
        Status = status,
        KeyIdMasked = KeyIdMask.Mask(NewKeyId),
        EncryptedAccessKeyId = _protector.Protect(NewKeyId, SecretPurposes.AccessKeyId),
        EncryptedSecretAccessKey = _protector.Protect(NewSecret, SecretPurposes.SecretAccessKey)
    };
}
