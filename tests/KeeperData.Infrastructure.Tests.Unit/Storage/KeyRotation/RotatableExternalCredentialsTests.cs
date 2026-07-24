using Amazon.Runtime;
using Amazon.S3;
using FluentAssertions;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
using KeeperData.Infrastructure.Storage.KeyRotation;
using KeeperData.Infrastructure.Tests.Unit.Storage.Factories;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Storage.KeyRotation;

public class RotatableExternalCredentialsTests
{
    [Fact]
    public void GetCredentials_BeforeProviderAttached_ReturnsFallback()
    {
        // Arrange
        var sut = new RotatableExternalCredentials("FALLBACKKEY123456789", "fallback-secret");

        // Act
        var credentials = sut.GetCredentials();

        // Assert
        credentials.AccessKey.Should().Be("FALLBACKKEY123456789");
        credentials.SecretKey.Should().Be("fallback-secret");
    }

    [Fact]
    public void GetCredentials_AfterProviderAttached_DelegatesToProvider()
    {
        // Arrange
        var rotated = new ImmutableCredentials("AKIAROTATED123456789", "rotated-secret", null);
        var providerMock = new Mock<IExternalStorageCredentialsProvider>();
        providerMock.Setup(p => p.GetCurrent()).Returns(rotated);

        var sut = new RotatableExternalCredentials("FALLBACKKEY123456789", "fallback-secret");
        sut.AttachProvider(providerMock.Object);

        // Act
        var credentials = sut.GetCredentials();

        // Assert
        credentials.AccessKey.Should().Be("AKIAROTATED123456789");
        credentials.SecretKey.Should().Be("rotated-secret");
    }

    [Fact]
    public async Task GetCredentialsAsync_MatchesSyncBehaviour()
    {
        // Arrange
        var sut = new RotatableExternalCredentials("FALLBACKKEY123456789", "fallback-secret");

        // Act
        var credentials = await sut.GetCredentialsAsync();

        // Assert
        credentials.AccessKey.Should().Be("FALLBACKKEY123456789");
    }

    [Fact]
    public void S3ClientFactory_AddClientWithAwsCredentials_RegistersClient()
    {
        // Arrange
        var factory = new S3ClientFactory();
        var credentials = new RotatableExternalCredentials("FALLBACKKEY123456789", "fallback-secret");
        var config = new AmazonS3Config { RegionEndpoint = Amazon.RegionEndpoint.EUWest2 };

        // Act
        factory.AddClientWithCredentials<TestStorageClient>("bucket", credentials, config);

        // Assert
        factory.HasStorageClient(typeof(TestStorageClient).Name).Should().BeTrue();
        factory.GetClientBucketName<TestStorageClient>().Should().Be("bucket");
    }

    [Fact]
    public void S3ClientFactory_AddClientWithAwsCredentials_WithoutBucketName_Throws()
    {
        // Arrange
        var factory = new S3ClientFactory();
        var credentials = new RotatableExternalCredentials("key", "secret");

        // Act
        var act = () => factory.AddClientWithCredentials<TestStorageClient>(
            "", credentials, new AmazonS3Config { RegionEndpoint = Amazon.RegionEndpoint.EUWest2 });

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*bucket name*");
    }
}
