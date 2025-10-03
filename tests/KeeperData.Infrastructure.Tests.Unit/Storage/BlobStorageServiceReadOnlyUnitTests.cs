using Amazon.S3;
using FluentAssertions;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Storage;

/// <summary>
/// Tests `BlobStorageServiceReadOnly` - Basic functionality tests that don't require LocalStack
/// </summary>
public class BlobStorageServiceReadOnlyUnitTests
{
    private readonly Mock<ILogger<S3BlobStorageServiceReadOnly>> _loggerMock;
    private readonly Mock<IAmazonS3> _mockS3Client;
    private const string TestContainer = "test-bucket";

    public BlobStorageServiceReadOnlyUnitTests()
    {
        _loggerMock = new Mock<ILogger<S3BlobStorageServiceReadOnly>>();
        _mockS3Client = new Mock<IAmazonS3>();
    }

    [Fact]
    public void Constructor_WithNullS3Client_ShouldThrow()
    {
        // Act & Assert
        var act = () => new S3BlobStorageServiceReadOnly(
            (IAmazonS3)null!,
            _loggerMock.Object,
            TestContainer);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("s3Client");
    }

    [Fact]
    public void Constructor_WithNullLogger_ShouldThrow()
    {
        // Act & Assert
        var act = () => new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            null!,
            TestContainer);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("logger");
    }

    [Fact]
    public void Constructor_WithNullContainer_ShouldThrow()
    {
        // Act & Assert
        var act = () => new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            null!);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("bucketName");
    }

    [Fact]
    public void Constructor_WithValidParameters_ShouldCreateInstance()
    {
        // Arrange & Act
        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer);

        // Assert
        service.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithTopLevelFolder_ShouldCreateInstance()
    {
        // Arrange & Act
        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer,
            "test-folder");

        // Assert
        service.Should().NotBeNull();
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    [InlineData("   ")]
    public void Constructor_WithEmptyTopLevelFolder_ShouldCreateInstance(string? topLevelFolder)
    {
        // Arrange & Act
        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer,
            topLevelFolder);

        // Assert
        service.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithServiceUrlOverload_ShouldCreateInstance()
    {
        // Arrange & Act
        var config = new AmazonS3Config
        {
            ServiceURL = "http://localhost:4566",
            ForcePathStyle = true,
            UseHttp = true,
            RegionEndpoint = Amazon.RegionEndpoint.USEast1
        };

        using var service = new S3BlobStorageServiceReadOnly(
            "test",
            "test",
            config,
            _loggerMock.Object,
            TestContainer);

        // Assert
        service.Should().NotBeNull();
    }
}