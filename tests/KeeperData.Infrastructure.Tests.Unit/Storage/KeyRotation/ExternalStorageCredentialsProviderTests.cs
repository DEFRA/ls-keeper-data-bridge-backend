using FluentAssertions;
using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Storage.KeyRotation;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.KeyRotation;
using KeeperData.Infrastructure.Storage.KeyRotation.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Storage.KeyRotation;

public class ExternalStorageCredentialsProviderTests : IDisposable
{
    private const string AccessKeyEnvVar = "PROVIDER_TESTS_ACCESS_KEY";
    private const string SecretKeyEnvVar = "PROVIDER_TESTS_SECRET_KEY";
    private const string FallbackAccessKey = "FALLBACKAKIA00000001";
    private const string FallbackSecretKey = "fallback-secret";

    private readonly Mock<IKeyRotationRepository> _repositoryMock = new();
    private readonly FakeTimeProvider _timeProvider = new();
    private readonly AesGcmSecretProtector _protector;
    private readonly ExternalStorageKeyRotationOptions _options = new() { CredentialsCacheSeconds = 300 };

    public ExternalStorageCredentialsProviderTests()
    {
        Environment.SetEnvironmentVariable(AccessKeyEnvVar, FallbackAccessKey);
        Environment.SetEnvironmentVariable(SecretKeyEnvVar, FallbackSecretKey);

        var key = new byte[32];
        for (var i = 0; i < key.Length; i++) key[i] = (byte)(i + 1);
        _protector = AesGcmSecretProtector.FromKey(key);
    }

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(AccessKeyEnvVar, null);
        Environment.SetEnvironmentVariable(SecretKeyEnvVar, null);
        GC.SuppressFinalize(this);
    }

    private ExternalStorageCredentialsProvider CreateSut(ISecretProtector? protector = null) => new(
        _repositoryMock.Object,
        protector ?? _protector,
        _options,
        new StorageConfiguration
        {
            ExternalStorage = new StorageWithCredentialsConfiguration
            {
                BucketName = "cerespfm-dev-dev1-livestockfeeds",
                AccessKeySecretName = AccessKeyEnvVar,
                SecretKeySecretName = SecretKeyEnvVar
            },
            SourceExternalPrefix = "litprd",
            SourceInternalPrefix = "qasrc",
            TargetInternalPrefix = "dest"
        },
        _timeProvider,
        Mock.Of<ILogger<ExternalStorageCredentialsProvider>>());

    private KeyRotationRecord CreateActiveRecord(string accessKeyId, string secretAccessKey) => new()
    {
        Id = Guid.NewGuid().ToString(),
        BucketName = "cerespfm-dev-dev1-livestockfeeds",
        RotatedAtUtc = _timeProvider.GetUtcNow().UtcDateTime,
        Source = KeyRotationSource.Automatic,
        Status = KeyRotationStatus.Active,
        KeyIdMasked = KeyIdMask.Mask(accessKeyId),
        EncryptedAccessKeyId = _protector.Protect(accessKeyId, SecretPurposes.AccessKeyId),
        EncryptedSecretAccessKey = _protector.Protect(secretAccessKey, SecretPurposes.SecretAccessKey)
    };

    [Fact]
    public void GetCurrent_WhenProtectorNotConfigured_ReturnsFallbackWithoutTouchingMongo()
    {
        // Arrange
        var sut = CreateSut(AesGcmSecretProtector.Unconfigured());

        // Act
        var credentials = sut.GetCurrent();

        // Assert
        credentials.AccessKey.Should().Be(FallbackAccessKey);
        credentials.SecretKey.Should().Be(FallbackSecretKey);
        _repositoryMock.Verify(r => r.GetActiveAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public void GetCurrent_WithActiveRotation_ReturnsDecryptedRotatedCredentials()
    {
        // Arrange
        _repositoryMock.Setup(r => r.GetActiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateActiveRecord("AKIAROTATED123456789", "rotated-secret"));
        var sut = CreateSut();

        // Act
        var credentials = sut.GetCurrent();

        // Assert
        credentials.AccessKey.Should().Be("AKIAROTATED123456789");
        credentials.SecretKey.Should().Be("rotated-secret");
    }

    [Fact]
    public void GetCurrent_WithinCacheTtl_QueriesMongoOnlyOnce()
    {
        // Arrange
        _repositoryMock.Setup(r => r.GetActiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateActiveRecord("AKIAROTATED123456789", "rotated-secret"));
        var sut = CreateSut();

        // Act
        sut.GetCurrent();
        sut.GetCurrent();

        // Assert
        _repositoryMock.Verify(r => r.GetActiveAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public void GetCurrent_AfterCacheExpiry_QueriesMongoAgain()
    {
        // Arrange
        _repositoryMock.Setup(r => r.GetActiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateActiveRecord("AKIAROTATED123456789", "rotated-secret"));
        var sut = CreateSut();

        // Act
        sut.GetCurrent();
        _timeProvider.Advance(TimeSpan.FromSeconds(_options.CredentialsCacheSeconds + 1));
        sut.GetCurrent();

        // Assert
        _repositoryMock.Verify(r => r.GetActiveAsync(It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public void GetCurrent_AfterInvalidate_QueriesMongoAgain()
    {
        // Arrange
        _repositoryMock.Setup(r => r.GetActiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateActiveRecord("AKIAROTATED123456789", "rotated-secret"));
        var sut = CreateSut();

        // Act
        sut.GetCurrent();
        sut.Invalidate();
        sut.GetCurrent();

        // Assert
        _repositoryMock.Verify(r => r.GetActiveAsync(It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public void GetCurrent_WithNoActiveRotation_ReturnsFallback()
    {
        // Arrange
        _repositoryMock.Setup(r => r.GetActiveAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((KeyRotationRecord?)null);
        var sut = CreateSut();

        // Act
        var credentials = sut.GetCurrent();

        // Assert
        credentials.AccessKey.Should().Be(FallbackAccessKey);
        credentials.SecretKey.Should().Be(FallbackSecretKey);
    }

    [Fact]
    public void GetCurrent_WhenMongoFails_FallsBackWithoutThrowing()
    {
        // Arrange
        _repositoryMock.Setup(r => r.GetActiveAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("mongo down"));
        var sut = CreateSut();

        // Act
        var credentials = sut.GetCurrent();

        // Assert
        credentials.AccessKey.Should().Be(FallbackAccessKey);
        credentials.SecretKey.Should().Be(FallbackSecretKey);
    }

    [Fact]
    public void GetCurrent_AfterMongoFailure_RetriesAfterShortFailureCache()
    {
        // Arrange
        _repositoryMock.SetupSequence(r => r.GetActiveAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("mongo down"))
            .ReturnsAsync(CreateActiveRecord("AKIAROTATED123456789", "rotated-secret"));
        var sut = CreateSut();

        // Act
        var first = sut.GetCurrent();
        _timeProvider.Advance(TimeSpan.FromSeconds(31));
        var second = sut.GetCurrent();

        // Assert
        first.AccessKey.Should().Be(FallbackAccessKey);
        second.AccessKey.Should().Be("AKIAROTATED123456789");
    }
}
