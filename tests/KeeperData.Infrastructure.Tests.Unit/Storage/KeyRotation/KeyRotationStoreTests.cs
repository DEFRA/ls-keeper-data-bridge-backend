using FluentAssertions;
using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Storage.KeyRotation;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.Storage.KeyRotation;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Storage.KeyRotation;

public class KeyRotationStoreTests
{
    private const string BucketName = "cerespfm-dev-dev1-livestockfeeds";
    private const string KeyId = "AKIANEWKEY1234567890";
    private const string Secret = "new-secret-value";

    private readonly Mock<IKeyRotationRepository> _repositoryMock = new();
    private readonly Mock<IExternalStorageCredentialsProvider> _providerMock = new();
    private readonly FakeTimeProvider _timeProvider = new();
    private readonly AesGcmSecretProtector _protector;

    public KeyRotationStoreTests()
    {
        var key = new byte[32];
        for (var i = 0; i < key.Length; i++) key[i] = (byte)(i + 1);
        _protector = AesGcmSecretProtector.FromKey(key);

        _timeProvider.SetUtcNow(new DateTimeOffset(2026, 7, 24, 3, 0, 0, TimeSpan.Zero));
    }

    private KeyRotationStore CreateSut(ISecretProtector? protector = null) =>
        new(_repositoryMock.Object, protector ?? _protector, _providerMock.Object, _timeProvider);

    [Fact]
    public void IsEncryptionConfigured_ReflectsProtector()
    {
        // Assert
        CreateSut().IsEncryptionConfigured.Should().BeTrue();
        CreateSut(AesGcmSecretProtector.Unconfigured()).IsEncryptionConfigured.Should().BeFalse();
    }

    [Fact]
    public async Task ActivateValidated_BuildsEncryptedRecordActivatesAndInvalidatesCache()
    {
        // Arrange
        KeyRotationRecord? activated = null;
        _repositoryMock.Setup(r => r.ActivateAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()))
            .Callback<KeyRotationRecord, CancellationToken>((r, _) => activated = r)
            .Returns(Task.CompletedTask);
        var sut = CreateSut();

        // Act
        var record = await sut.ActivateValidatedAsync(new ValidatedRotation(
            BucketName, KeyRotationSource.Automatic, KeyId, Secret,
            FileKey: "Dev1_LI_CDP_Int_User_accessKeys.csv", FileHash: "abc123"));

        // Assert
        activated.Should().BeSameAs(record);
        record.Id.Should().NotBeNullOrWhiteSpace();
        record.BucketName.Should().Be(BucketName);
        record.Source.Should().Be(KeyRotationSource.Automatic);
        record.Status.Should().Be(KeyRotationStatus.Active);
        record.FileKey.Should().Be("Dev1_LI_CDP_Int_User_accessKeys.csv");
        record.FileHash.Should().Be("abc123");
        record.KeyIdMasked.Should().Be("AKI...890");
        record.RotatedAtUtc.Should().Be(_timeProvider.GetUtcNow().UtcDateTime);
        record.ValidatedAtUtc.Should().Be(_timeProvider.GetUtcNow().UtcDateTime);

        // Credentials are stored encrypted and round-trip back to the originals.
        record.EncryptedAccessKeyId.Should().NotBeNull();
        record.EncryptedSecretAccessKey.Should().NotBeNull();
        _protector.Unprotect(record.EncryptedAccessKeyId!, SecretPurposes.AccessKeyId).Should().Be(KeyId);
        _protector.Unprotect(record.EncryptedSecretAccessKey!, SecretPurposes.SecretAccessKey).Should().Be(Secret);

        _providerMock.Verify(p => p.Invalidate(), Times.Once);
    }

    [Fact]
    public async Task ActivateValidated_WithRollbackSource_CarriesRolledBackFromId()
    {
        // Arrange
        var sut = CreateSut();

        // Act
        var record = await sut.ActivateValidatedAsync(new ValidatedRotation(
            BucketName, KeyRotationSource.Rollback, KeyId, Secret, RolledBackFromId: "previous-id"));

        // Assert
        record.Source.Should().Be(KeyRotationSource.Rollback);
        record.RolledBackFromId.Should().Be("previous-id");
        record.FileKey.Should().BeNull();
        record.FileHash.Should().BeNull();
    }

    [Fact]
    public async Task RecordFailed_AppendsFailedRecordWithoutKeyMaterialOrCacheInvalidation()
    {
        // Arrange
        KeyRotationRecord? failed = null;
        _repositoryMock.Setup(r => r.AddFailedAsync(It.IsAny<KeyRotationRecord>(), It.IsAny<CancellationToken>()))
            .Callback<KeyRotationRecord, CancellationToken>((r, _) => failed = r)
            .Returns(Task.CompletedTask);
        var sut = CreateSut();

        // Act
        await sut.RecordFailedAsync(new FailedRotation(
            BucketName, "Dev1_LI_CDP_Int_User_accessKeys.csv", "abc123", "AKI...890", "rejected"));

        // Assert
        failed.Should().NotBeNull();
        failed!.Status.Should().Be(KeyRotationStatus.Failed);
        failed.Source.Should().Be(KeyRotationSource.Automatic);
        failed.FileHash.Should().Be("abc123");
        failed.KeyIdMasked.Should().Be("AKI...890");
        failed.FailureReason.Should().Be("rejected");
        failed.EncryptedAccessKeyId.Should().BeNull();
        failed.EncryptedSecretAccessKey.Should().BeNull();
        failed.ValidatedAtUtc.Should().BeNull();
        _providerMock.Verify(p => p.Invalidate(), Times.Never);
    }

    [Fact]
    public async Task DecryptCredentials_RoundTripsStoredValues()
    {
        // Arrange
        var sut = CreateSut();
        var record = await sut.ActivateValidatedAsync(new ValidatedRotation(
            BucketName, KeyRotationSource.Manual, KeyId, Secret));

        // Act
        var (accessKeyId, secretAccessKey) = sut.DecryptCredentials(record);

        // Assert
        accessKeyId.Should().Be(KeyId);
        secretAccessKey.Should().Be(Secret);
    }

    [Fact]
    public void DecryptCredentials_WithoutKeyMaterial_Throws()
    {
        // Arrange
        var sut = CreateSut();
        var failedRecord = new KeyRotationRecord
        {
            Id = "failed",
            BucketName = BucketName,
            Status = KeyRotationStatus.Failed,
            KeyIdMasked = string.Empty
        };

        // Act
        var act = () => sut.DecryptCredentials(failedRecord);

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*no key material*");
    }

    [Fact]
    public async Task GetByIdAndLatestHash_PassThroughToRepository()
    {
        // Arrange
        var record = new KeyRotationRecord { Id = "x", BucketName = BucketName, KeyIdMasked = "AKI...890" };
        _repositoryMock.Setup(r => r.GetByIdAsync("x", It.IsAny<CancellationToken>())).ReturnsAsync(record);
        _repositoryMock.Setup(r => r.GetLatestObservedFileHashAsync(It.IsAny<CancellationToken>())).ReturnsAsync("hash");
        var sut = CreateSut();

        // Act & Assert
        (await sut.GetByIdAsync("x")).Should().BeSameAs(record);
        (await sut.GetLatestObservedFileHashAsync()).Should().Be("hash");
    }
}
