using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Logging;
using Moq;
using System.Text;

namespace KeeperData.Infrastructure.Tests.Unit.Storage;

/// <summary>
/// Unit tests for `BlobStorageService` - Tests write operations and functionality specific to the writable service
/// Read-only functionality is tested in `BlobStorageServiceReadOnlyUnitTests`
/// </summary>
public class BlobStorageServiceUnitTests
{
    private readonly Mock<ILogger<BlobStorageService>> _loggerMock;
    private readonly Mock<IAmazonS3> _mockS3Client;
    private const string TestContainer = "test-bucket";

    public BlobStorageServiceUnitTests()
    {
        _loggerMock = new Mock<ILogger<BlobStorageService>>();
        _mockS3Client = new Mock<IAmazonS3>();
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithNullS3Client_ShouldThrow()
    {
        // Act & Assert
        var act = () => new BlobStorageService(
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
        var act = () => new BlobStorageService(
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
        var act = () => new BlobStorageService(
            _mockS3Client.Object,
            _loggerMock.Object,
            null!);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("container");
    }

    [Fact]
    public void Constructor_WithValidParameters_ShouldCreateInstance()
    {
        // Arrange & Act
        using var service = new BlobStorageService(
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
        using var service = new BlobStorageService(
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
    [InlineData("\t")]
    [InlineData("\n")]
    public void Constructor_WithEmptyOrNullTopLevelFolder_ShouldCreateInstance(string? topLevelFolder)
    {
        // Arrange & Act
        using var service = new BlobStorageService(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer,
            topLevelFolder);

        // Assert
        service.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithConfigOverload_ShouldCreateInstance()
    {
        // Arrange & Act
        var config = new AmazonS3Config
        {
            ServiceURL = "http://localhost:4566",
            ForcePathStyle = true,
            UseHttp = true,
            RegionEndpoint = Amazon.RegionEndpoint.USEast1
        };

        using var service = new BlobStorageService(
            config,
            _loggerMock.Object,
            TestContainer);

        // Assert
        service.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithCredentialsOverload_ShouldCreateInstance()
    {
        // Arrange & Act
        var config = new AmazonS3Config
        {
            ServiceURL = "http://localhost:4566",
            ForcePathStyle = true,
            UseHttp = true
        };

        using var service = new BlobStorageService(
            "test-access-key",
            "test-secret-key",
            config,
            _loggerMock.Object,
            TestContainer);

        // Assert
        service.Should().NotBeNull();
    }

    #endregion

    #region UploadAsync Tests

    [Fact]
    public async Task UploadAsync_WithValidParameters_ShouldCallPutObjectAsync()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";
        var content = Encoding.UTF8.GetBytes("test content");
        var contentType = "text/plain";
        var metadata = new Dictionary<string, string> { ["key1"] = "value1" };

        PutObjectRequest? capturedRequest = null;
        _mockS3Client
            .Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
            .Callback<PutObjectRequest, CancellationToken>((req, ct) => capturedRequest = req)
            .ReturnsAsync(new PutObjectResponse());

        // Act
        await service.UploadAsync(objectKey, content, contentType, metadata);

        // Assert
        _mockS3Client.Verify(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()), Times.Once);

        capturedRequest.Should().NotBeNull();
        capturedRequest!.BucketName.Should().Be(TestContainer);
        capturedRequest.Key.Should().Be(objectKey);
        capturedRequest.ContentType.Should().Be(contentType);

        // AWS SDK automatically prefixes user metadata with "x-amz-meta-"
        capturedRequest.Metadata.Keys.Should().Contain("x-amz-meta-key1");
        capturedRequest.Metadata["x-amz-meta-key1"].Should().Be("value1");
    }

    [Fact]
    public async Task UploadAsync_WithTopLevelFolder_ShouldPrependFolderToKey()
    {
        // Arrange
        var topLevelFolder = "tenant-123";
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer, topLevelFolder);
        var objectKey = "test-key";
        var content = Encoding.UTF8.GetBytes("test content");

        _mockS3Client
            .Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PutObjectResponse());

        // Act
        await service.UploadAsync(objectKey, content);

        // Assert
        _mockS3Client.Verify(x => x.PutObjectAsync(
            It.Is<PutObjectRequest>(req =>
                req.BucketName == TestContainer &&
                req.Key == $"{topLevelFolder}/{objectKey}"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UploadAsync_WithoutContentType_ShouldUseDefaultContentType()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";
        var content = Encoding.UTF8.GetBytes("test content");

        _mockS3Client
            .Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PutObjectResponse());

        // Act
        await service.UploadAsync(objectKey, content);

        // Assert
        _mockS3Client.Verify(x => x.PutObjectAsync(
            It.Is<PutObjectRequest>(req => req.ContentType == "application/octet-stream"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UploadAsync_WhenS3ClientThrows_ShouldLogErrorAndRethrow()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";
        var content = Encoding.UTF8.GetBytes("test content");
        var exception = new AmazonS3Exception("S3 Error");

        _mockS3Client
            .Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

        // Act & Assert
        var act = () => service.UploadAsync(objectKey, content);
        await act.Should().ThrowAsync<AmazonS3Exception>();

        // Verify error logging
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Failed to upload object")),
                exception,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    #endregion

    #region OpenWriteAsync Tests

    [Fact]
    public async Task OpenWriteAsync_WithValidParameters_ShouldReturnMultipartStream()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";
        var contentType = "application/octet-stream";
        var metadata = new Dictionary<string, string> { ["key1"] = "value1" };

        InitiateMultipartUploadRequest? capturedRequest = null;
        _mockS3Client
            .Setup(x => x.InitiateMultipartUploadAsync(It.IsAny<InitiateMultipartUploadRequest>(), It.IsAny<CancellationToken>()))
            .Callback<InitiateMultipartUploadRequest, CancellationToken>((req, ct) => capturedRequest = req)
            .ReturnsAsync(new InitiateMultipartUploadResponse { UploadId = "test-upload-id" });

        // Act
        var stream = await service.OpenWriteAsync(objectKey, contentType, metadata);

        // Assert
        stream.Should().NotBeNull();
        stream.CanWrite.Should().BeTrue();
        stream.CanRead.Should().BeFalse();
        stream.CanSeek.Should().BeFalse();

        // Verify multipart upload initiation
        _mockS3Client.Verify(x => x.InitiateMultipartUploadAsync(It.IsAny<InitiateMultipartUploadRequest>(), It.IsAny<CancellationToken>()), Times.Once);

        capturedRequest.Should().NotBeNull();
        capturedRequest!.BucketName.Should().Be(TestContainer);
        capturedRequest.Key.Should().Be(objectKey);
        capturedRequest.ContentType.Should().Be(contentType);

        // AWS SDK automatically prefixes user metadata with "x-amz-meta-"
        capturedRequest.Metadata.Keys.Should().Contain("x-amz-meta-key1");
        capturedRequest.Metadata["x-amz-meta-key1"].Should().Be("value1");

        // Clean up
        await stream.DisposeAsync();
    }

    [Fact]
    public async Task OpenWriteAsync_WithTopLevelFolder_ShouldPrependFolderToKey()
    {
        // Arrange
        var topLevelFolder = "tenant-456";
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer, topLevelFolder);
        var objectKey = "test-key";

        _mockS3Client
            .Setup(x => x.InitiateMultipartUploadAsync(It.IsAny<InitiateMultipartUploadRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new InitiateMultipartUploadResponse { UploadId = "test-upload-id" });

        // Act
        var stream = await service.OpenWriteAsync(objectKey);

        // Assert
        _mockS3Client.Verify(x => x.InitiateMultipartUploadAsync(
            It.Is<InitiateMultipartUploadRequest>(req =>
                req.BucketName == TestContainer &&
                req.Key == $"{topLevelFolder}/{objectKey}"),
            It.IsAny<CancellationToken>()), Times.Once);

        // Clean up
        await stream.DisposeAsync();
    }

    [Fact]
    public async Task OpenWriteAsync_WithoutContentType_ShouldUseDefaultContentType()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";

        _mockS3Client
            .Setup(x => x.InitiateMultipartUploadAsync(It.IsAny<InitiateMultipartUploadRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new InitiateMultipartUploadResponse { UploadId = "test-upload-id" });

        // Act
        var stream = await service.OpenWriteAsync(objectKey);

        // Assert
        _mockS3Client.Verify(x => x.InitiateMultipartUploadAsync(
            It.Is<InitiateMultipartUploadRequest>(req => req.ContentType == "application/octet-stream"),
            It.IsAny<CancellationToken>()), Times.Once);

        // Clean up
        await stream.DisposeAsync();
    }

    [Fact]
    public async Task OpenWriteAsync_WhenInitiateMultipartFails_ShouldLogErrorAndRethrow()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";
        var exception = new AmazonS3Exception("Failed to initiate multipart upload");

        _mockS3Client
            .Setup(x => x.InitiateMultipartUploadAsync(It.IsAny<InitiateMultipartUploadRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

        // Act & Assert
        var act = () => service.OpenWriteAsync(objectKey);
        await act.Should().ThrowAsync<AmazonS3Exception>();

        // Verify error logging
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Failed to open write stream")),
                exception,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    #endregion

    #region SetMetadataAsync Tests

    [Fact]
    public async Task SetMetadataAsync_WithValidParameters_ShouldCallCopyObjectAsync()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";
        var metadata = new Dictionary<string, string>
        {
            ["key1"] = "value1",
            ["key2"] = "value2"
        };

        CopyObjectRequest? capturedRequest = null;
        _mockS3Client
            .Setup(x => x.CopyObjectAsync(It.IsAny<CopyObjectRequest>(), It.IsAny<CancellationToken>()))
            .Callback<CopyObjectRequest, CancellationToken>((req, ct) => capturedRequest = req)
            .ReturnsAsync(new CopyObjectResponse());

        // Act
        await service.SetMetadataAsync(objectKey, metadata);

        // Assert
        _mockS3Client.Verify(x => x.CopyObjectAsync(It.IsAny<CopyObjectRequest>(), It.IsAny<CancellationToken>()), Times.Once);

        capturedRequest.Should().NotBeNull();
        capturedRequest!.SourceBucket.Should().Be(TestContainer);
        capturedRequest.SourceKey.Should().Be(objectKey);
        capturedRequest.DestinationBucket.Should().Be(TestContainer);
        capturedRequest.DestinationKey.Should().Be(objectKey);
        capturedRequest.MetadataDirective.Should().Be(S3MetadataDirective.REPLACE);

        // AWS SDK automatically prefixes user metadata with "x-amz-meta-"
        capturedRequest.Metadata.Keys.Should().Contain("x-amz-meta-key1");
        capturedRequest.Metadata["x-amz-meta-key1"].Should().Be("value1");
        capturedRequest.Metadata.Keys.Should().Contain("x-amz-meta-key2");
        capturedRequest.Metadata["x-amz-meta-key2"].Should().Be("value2");
    }

    [Fact]
    public async Task SetMetadataAsync_WithTopLevelFolder_ShouldPrependFolderToKey()
    {
        // Arrange
        var topLevelFolder = "company-xyz";
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer, topLevelFolder);
        var objectKey = "test-key";
        var metadata = new Dictionary<string, string> { ["key1"] = "value1" };

        _mockS3Client
            .Setup(x => x.CopyObjectAsync(It.IsAny<CopyObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new CopyObjectResponse());

        // Act
        await service.SetMetadataAsync(objectKey, metadata);

        // Assert
        var expectedFullKey = $"{topLevelFolder}/{objectKey}";
        _mockS3Client.Verify(x => x.CopyObjectAsync(
            It.Is<CopyObjectRequest>(req =>
                req.SourceBucket == TestContainer &&
                req.SourceKey == expectedFullKey &&
                req.DestinationBucket == TestContainer &&
                req.DestinationKey == expectedFullKey),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SetMetadataAsync_WhenCopyObjectFails_ShouldLogErrorAndRethrow()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";
        var metadata = new Dictionary<string, string> { ["key1"] = "value1" };
        var exception = new AmazonS3Exception("Copy failed");

        _mockS3Client
            .Setup(x => x.CopyObjectAsync(It.IsAny<CopyObjectRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

        // Act & Assert
        var act = () => service.SetMetadataAsync(objectKey, metadata);
        await act.Should().ThrowAsync<AmazonS3Exception>();

        // Verify error logging
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Failed to update metadata")),
                exception,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    #endregion

    #region DeleteAsync Tests

    [Fact]
    public async Task DeleteAsync_WithValidParameters_ShouldCallDeleteObjectAsync()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";

        _mockS3Client
            .Setup(x => x.DeleteObjectAsync(It.IsAny<DeleteObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteObjectResponse());

        // Act
        await service.DeleteAsync(objectKey);

        // Assert
        _mockS3Client.Verify(x => x.DeleteObjectAsync(
            It.Is<DeleteObjectRequest>(req =>
                req.BucketName == TestContainer &&
                req.Key == objectKey),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task DeleteAsync_WithTopLevelFolder_ShouldPrependFolderToKey()
    {
        // Arrange
        var topLevelFolder = "department-abc";
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer, topLevelFolder);
        var objectKey = "test-key";

        _mockS3Client
            .Setup(x => x.DeleteObjectAsync(It.IsAny<DeleteObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteObjectResponse());

        // Act
        await service.DeleteAsync(objectKey);

        // Assert
        _mockS3Client.Verify(x => x.DeleteObjectAsync(
            It.Is<DeleteObjectRequest>(req =>
                req.BucketName == TestContainer &&
                req.Key == $"{topLevelFolder}/{objectKey}"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task DeleteAsync_WhenDeleteObjectFails_ShouldLogErrorAndRethrow()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";
        var exception = new AmazonS3Exception("Delete failed");

        _mockS3Client
            .Setup(x => x.DeleteObjectAsync(It.IsAny<DeleteObjectRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

        // Act & Assert
        var act = () => service.DeleteAsync(objectKey);
        await act.Should().ThrowAsync<AmazonS3Exception>();

        // Verify error logging
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Failed to delete object")),
                exception,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    #endregion

    #region Top-Level Folder Normalization Tests

    [Theory]
    [InlineData("simple", "simple/")]
    [InlineData("simple/", "simple/")]
    [InlineData("/simple", "simple/")]
    [InlineData("/simple/", "simple/")]
    [InlineData("//simple//", "simple/")]
    [InlineData("  nested/folder/path  ", "nested/folder/path/")]
    [InlineData("/nested/folder/path/", "nested/folder/path/")]
    public async Task Operations_WithVariousFolderFormats_ShouldNormalizeCorrectly(string inputFolder, string expectedNormalizedFolder)
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer, inputFolder);
        var objectKey = "test-key";
        var content = Encoding.UTF8.GetBytes("test");

        _mockS3Client
            .Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PutObjectResponse());

        // Act
        await service.UploadAsync(objectKey, content);

        // Assert
        var expectedFullKey = expectedNormalizedFolder + objectKey;
        _mockS3Client.Verify(x => x.PutObjectAsync(
            It.Is<PutObjectRequest>(req => req.Key == expectedFullKey),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\t\n")]
    [InlineData(null)]
    public async Task Operations_WithEmptyOrNullFolder_ShouldNotPrependFolder(string? inputFolder)
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer, inputFolder);
        var objectKey = "test-key";
        var content = Encoding.UTF8.GetBytes("test");

        _mockS3Client
            .Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PutObjectResponse());

        // Act
        await service.UploadAsync(objectKey, content);

        // Assert - Should use the original key without any prefix
        _mockS3Client.Verify(x => x.PutObjectAsync(
            It.Is<PutObjectRequest>(req => req.Key == objectKey),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    #endregion

    #region Debug Logging Tests

    [Fact]
    public async Task UploadAsync_OnSuccess_ShouldLogDebugMessage()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";
        var content = Encoding.UTF8.GetBytes("test");

        _mockS3Client
            .Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PutObjectResponse());

        // Act
        await service.UploadAsync(objectKey, content);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Successfully uploaded object")),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task DeleteAsync_OnSuccess_ShouldLogDebugMessage()
    {
        // Arrange
        using var service = new BlobStorageService(_mockS3Client.Object, _loggerMock.Object, TestContainer);
        var objectKey = "test-key";

        _mockS3Client
            .Setup(x => x.DeleteObjectAsync(It.IsAny<DeleteObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteObjectResponse());

        // Act
        await service.DeleteAsync(objectKey);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Successfully deleted object")),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    #endregion
}