using Amazon.S3;
using Amazon.S3.Model;
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

    #region Constructor Tests

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

    #endregion

    #region ListPageAsync Tests

    [Fact]
    public async Task ListPageAsync_WithEmptyBucket_ReturnsEmptyList()
    {
        // Arrange
        _mockS3Client.Setup(x => x.ListObjectsV2Async(
                It.IsAny<ListObjectsV2Request>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response
            {
                S3Objects = new List<S3Object>(),
                IsTruncated = false
            });

        _mockS3Client.Setup(x => x.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Returns("https://test-url.com/object");

        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer);

        // Act
        var result = await service.ListPageAsync();

        // Assert
        result.Items.Should().BeEmpty();
        result.IsTruncated.Should().BeFalse();
        result.ContinuationToken.Should().BeNull();
    }

    [Fact]
    public async Task ListPageAsync_WithObjects_ReturnsCorrectItems()
    {
        // Arrange
        var s3Objects = new List<S3Object>
        {
            new() { Key = "file1.txt", Size = 100, ETag = "\"etag1\"", LastModified = DateTime.UtcNow },
            new() { Key = "file2.txt", Size = 200, ETag = "\"etag2\"", LastModified = DateTime.UtcNow }
        };

        _mockS3Client.Setup(x => x.ListObjectsV2Async(
                It.IsAny<ListObjectsV2Request>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response
            {
                S3Objects = s3Objects,
                IsTruncated = false
            });

        _mockS3Client.Setup(x => x.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Returns("https://test-url.com/object");

        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer);

        // Act
        var result = await service.ListPageAsync();

        // Assert
        result.Items.Should().HaveCount(2);
        result.Items[0].Key.Should().Be("file1.txt");
        result.Items[0].Size.Should().Be(100);
        result.Items[0].ETag.Should().Be("etag1");
        result.Items[1].Key.Should().Be("file2.txt");
        result.Items[1].Size.Should().Be(200);
    }

    [Fact]
    public async Task ListPageAsync_WithPrefix_PassesPrefixToRequest()
    {
        // Arrange
        ListObjectsV2Request? capturedRequest = null;
        _mockS3Client.Setup(x => x.ListObjectsV2Async(
                It.IsAny<ListObjectsV2Request>(),
                It.IsAny<CancellationToken>()))
            .Callback<ListObjectsV2Request, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new ListObjectsV2Response
            {
                S3Objects = new List<S3Object>(),
                IsTruncated = false
            });

        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer);

        // Act
        await service.ListPageAsync(prefix: "my-prefix/");

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.Prefix.Should().Be("my-prefix/");
    }

    [Fact]
    public async Task ListPageAsync_WithTopLevelFolder_PrependsFolderToPrefix()
    {
        // Arrange
        ListObjectsV2Request? capturedRequest = null;
        _mockS3Client.Setup(x => x.ListObjectsV2Async(
                It.IsAny<ListObjectsV2Request>(),
                It.IsAny<CancellationToken>()))
            .Callback<ListObjectsV2Request, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new ListObjectsV2Response
            {
                S3Objects = new List<S3Object>(),
                IsTruncated = false
            });

        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer,
            "top-folder/");

        // Act
        await service.ListPageAsync(prefix: "sub-prefix/");

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.Prefix.Should().Be("top-folder/sub-prefix/");
    }

    [Fact]
    public async Task ListPageAsync_WithPagination_ReturnsContinuationToken()
    {
        // Arrange
        _mockS3Client.Setup(x => x.ListObjectsV2Async(
                It.IsAny<ListObjectsV2Request>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response
            {
                S3Objects = new List<S3Object> { new() { Key = "file1.txt" } },
                IsTruncated = true,
                NextContinuationToken = "next-token-123"
            });

        _mockS3Client.Setup(x => x.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Returns("https://test-url.com/object");

        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer);

        // Act
        var result = await service.ListPageAsync(pageSize: 1);

        // Assert
        result.IsTruncated.Should().BeTrue();
        result.ContinuationToken.Should().Be("next-token-123");
    }

    [Fact]
    public async Task ListPageAsync_EnforcesMaxPageSize()
    {
        // Arrange
        ListObjectsV2Request? capturedRequest = null;
        _mockS3Client.Setup(x => x.ListObjectsV2Async(
                It.IsAny<ListObjectsV2Request>(),
                It.IsAny<CancellationToken>()))
            .Callback<ListObjectsV2Request, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new ListObjectsV2Response
            {
                S3Objects = new List<S3Object>(),
                IsTruncated = false
            });

        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer);

        // Act
        await service.ListPageAsync(pageSize: 5000); // Request more than max

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.MaxKeys.Should().Be(1000); // Should be capped at 1000
    }

    #endregion

    #region GetMetadataAsync Tests

    [Fact]
    public async Task GetMetadataAsync_WithValidKey_ReturnsMetadata()
    {
        // Arrange
        var lastModified = DateTime.UtcNow;
        _mockS3Client.Setup(x => x.GetObjectMetadataAsync(
                It.IsAny<GetObjectMetadataRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetObjectMetadataResponse
            {
                ContentLength = 1024,
                ETag = "\"test-etag\"",
                LastModified = lastModified,
                Headers = { ContentType = "application/json" }
            });

        _mockS3Client.Setup(x => x.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Returns("https://test-url.com/object");

        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer);

        // Act
        var result = await service.GetMetadataAsync("test-key.json");

        // Assert
        result.ContentLength.Should().Be(1024);
        result.ETag.Should().Be("test-etag");
        result.ContentType.Should().Be("application/json");
        result.Key.Should().Be("test-key.json");
        result.Container.Should().Be(TestContainer);
    }

    [Fact]
    public async Task GetMetadataAsync_WithTopLevelFolder_PrependsFolder()
    {
        // Arrange
        GetObjectMetadataRequest? capturedRequest = null;
        _mockS3Client.Setup(x => x.GetObjectMetadataAsync(
                It.IsAny<GetObjectMetadataRequest>(),
                It.IsAny<CancellationToken>()))
            .Callback<GetObjectMetadataRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new GetObjectMetadataResponse
            {
                ContentLength = 100
            });

        _mockS3Client.Setup(x => x.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Returns("https://test-url.com/object");

        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer,
            "prefix/");

        // Act
        await service.GetMetadataAsync("file.txt");

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.Key.Should().Be("prefix/file.txt");
    }

    #endregion

    #region ExistsAsync Tests

    [Fact]
    public async Task ExistsAsync_WhenObjectExists_ReturnsTrue()
    {
        // Arrange
        _mockS3Client.Setup(x => x.GetObjectMetadataAsync(
                It.IsAny<GetObjectMetadataRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetObjectMetadataResponse());

        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer);

        // Act
        var result = await service.ExistsAsync("existing-file.txt");

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_WhenObjectDoesNotExist_ReturnsFalse()
    {
        // Arrange
        _mockS3Client.Setup(x => x.GetObjectMetadataAsync(
                It.IsAny<GetObjectMetadataRequest>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonS3Exception("Not Found") { StatusCode = System.Net.HttpStatusCode.NotFound });

        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer);

        // Act
        var result = await service.ExistsAsync("non-existing-file.txt");

        // Assert
        result.Should().BeFalse();
    }

    #endregion

    #region DownloadAsync Tests

    [Fact]
    public async Task DownloadAsync_WithValidKey_ReturnsBytes()
    {
        // Arrange
        var content = "test content"u8.ToArray();
        var responseStream = new MemoryStream(content);

        _mockS3Client.Setup(x => x.GetObjectAsync(
                It.IsAny<GetObjectRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetObjectResponse
            {
                ResponseStream = responseStream,
                ContentLength = content.Length
            });

        using var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer);

        // Act
        var result = await service.DownloadAsync("test-file.txt");

        // Assert
        result.Should().NotBeNull();
        System.Text.Encoding.UTF8.GetString(result).Should().Be("test content");
    }

    #endregion

    #region Dispose Tests

    [Fact]
    public void Dispose_WhenCalledMultipleTimes_DoesNotThrow()
    {
        // Arrange
        var service = new S3BlobStorageServiceReadOnly(
            _mockS3Client.Object,
            _loggerMock.Object,
            TestContainer);

        // Act & Assert
        var act = () =>
        {
            service.Dispose();
            service.Dispose();
        };

        act.Should().NotThrow();
    }

    #endregion
}