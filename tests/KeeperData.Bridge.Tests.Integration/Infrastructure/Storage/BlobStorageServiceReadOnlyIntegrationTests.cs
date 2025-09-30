using Amazon.S3;
using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Logging;
using Moq;
using System.Text;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Infrastructure.Storage;

/// <summary>
/// Tests `BlobStorageServiceReadOnly` against localstack S3 in testcontainer
/// </summary>
[Collection("LocalStack")]
public class BlobStorageServiceReadOnlyIntegrationTests
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly LocalStackFixture _localStackFixture;
    private readonly Mock<ILogger<BlobStorageServiceReadOnly>> _loggerMock;
    private readonly BlobStorageServiceReadOnly _blobService;

    private const string TestTopLevelFolder = "test-folder";

    public BlobStorageServiceReadOnlyIntegrationTests(ITestOutputHelper testOutputHelper, LocalStackFixture localStackFixture)
    {
        _testOutputHelper = testOutputHelper;
        _localStackFixture = localStackFixture;
        _loggerMock = new Mock<ILogger<BlobStorageServiceReadOnly>>();

        // Create blob service using the shared S3 client
        _blobService = new BlobStorageServiceReadOnly(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
    }

    // Note: The following tests require LocalStack to be working properly
    // If LocalStack connectivity issues persist, these tests may be skipped in CI

    [Fact]
    public async Task ExistsAsync_ExistingObject_ShouldReturnTrue()
    {
        // Act
        var exists = await _blobService.ExistsAsync("small.txt");

        // Assert
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_NonExistentObject_ShouldReturnFalse()
    {
        // Act
        var exists = await _blobService.ExistsAsync("nonexistent.txt");

        // Assert
        exists.Should().BeFalse();
    }

    [Fact]
    public async Task ListAsync_WithoutPrefix_ShouldReturnAllObjects()
    {
        // Act
        var items = await _blobService.ListAsync();

        // Assert
        items.Should().HaveCount(6); // 5 test files + 1 large file
        items.Should().Contain(x => x.Key == "small.txt");
        items.Should().Contain(x => x.Key == "medium.txt");
        items.Should().Contain(x => x.Key == "subfolder/nested.txt");
        items.Should().Contain(x => x.Key == "large-file.bin");
    }

    [Fact]
    public async Task ListAsync_WithPrefix_ShouldReturnFilteredObjects()
    {
        // Act
        var items = await _blobService.ListAsync("sub");

        // Assert
        items.Should().HaveCount(1);
        items.First().Key.Should().Be("subfolder/nested.txt");
    }

    [Fact]
    public async Task ListPageAsync_ShouldReturnPagedResults()
    {
        // Act
        var page = await _blobService.ListPageAsync(pageSize: 2);

        // Assert
        page.Items.Should().HaveCount(2);
        page.IsTruncated.Should().BeTrue();
        page.ContinuationToken.Should().NotBeNullOrEmpty();

        // Get next page
        var nextPage = await _blobService.ListPageAsync(pageSize: 2, continuationToken: page.ContinuationToken);
        nextPage.Items.Should().NotBeEmpty();
    }

    [Fact]
    public async Task GetMetadataAsync_ExistingObject_ShouldReturnMetadata()
    {
        // Act
        var metadata = await _blobService.GetMetadataAsync("small.txt");

        // Assert
        metadata.Should().NotBeNull();
        metadata.Container.Should().Be(LocalStackFixture.TestBucket);
        metadata.Key.Should().Be("small.txt");
        metadata.ContentLength.Should().BeGreaterThan(0);
        metadata.ContentType.Should().Be("text/plain");
        metadata.UserMetadata.Should().ContainKey("x-amz-meta-test-meta");
        metadata.StorageUri.Should().NotBeNull();
        metadata.StorageUri.ToString().Should().StartWith("s3://");
    }

    [Fact]
    public async Task GetMetadataAsync_NonExistentObject_ShouldThrow()
    {
        // Act & Assert
        await _blobService.Invoking(x => x.GetMetadataAsync("nonexistent.txt"))
            .Should().ThrowAsync<AmazonS3Exception>();
    }

    [Fact]
    public async Task DownloadAsync_ExistingObject_ShouldReturnContent()
    {
        // Act
        var content = await _blobService.DownloadAsync("small.txt");

        // Assert
        content.Should().NotBeEmpty();
        Encoding.UTF8.GetString(content).Should().Be("Small test content");
    }

    [Fact]
    public async Task OpenReadAsync_ExistingObject_ShouldReturnReadableStream()
    {
        // Act
        using var stream = await _blobService.OpenReadAsync("medium.txt");

        // Assert
        stream.Should().NotBeNull();
        stream.CanRead.Should().BeTrue();

        using var reader = new StreamReader(stream);
        var content = await reader.ReadToEndAsync();
        content.Should().Contain("Medium test content");
    }

    [Fact]
    public async Task OpenReadAsync_LargeFile_ShouldStreamWithoutBuffering()
    {
        // Act
        using var stream = await _blobService.OpenReadAsync("large-file.bin");

        // Assert
        stream.Should().NotBeNull();
        stream.CanRead.Should().BeTrue();

        // Read in chunks to verify streaming works without loading everything into memory
        var buffer = new byte[8192];
        var totalRead = 0L;
        int bytesRead;
        var pattern = Encoding.UTF8.GetBytes("LARGE_FILE_TEST_PATTERN_REPEATED_");
        var patternOffset = 0;

        do
        {
            bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
            totalRead += bytesRead;

            if (bytesRead > 0)
            {
                // Verify pattern is preserved - account for pattern continuation across buffer boundaries
                for (var i = 0; i < bytesRead; i++)
                {
                    var expectedByte = pattern[patternOffset % pattern.Length];
                    buffer[i].Should().Be(expectedByte,
                        $"at position {totalRead - bytesRead + i}, expected pattern byte at index {patternOffset % pattern.Length}");
                    patternOffset++;
                }
            }
        } while (bytesRead > 0);

        // Should have read approximately 10MB
        totalRead.Should().BeGreaterThan(9 * 1024 * 1024);
        _testOutputHelper.WriteLine($"Successfully streamed {totalRead:N0} bytes");
    }

    [Fact]
    public async Task Constructor_WithTopLevelFolder_ShouldFilterOperations()
    {
        // Arrange
        using var service = new BlobStorageServiceReadOnly(
            _localStackFixture.S3Client,
            _loggerMock.Object,
            LocalStackFixture.TestBucket,
            TestTopLevelFolder);

        // Act
        var items = await service.ListAsync();

        // Assert
        items.Should().HaveCount(2); // Only files within test-folder/
        items.Should().OnlyContain(x => x.Key.StartsWith("inside-folder.txt") || x.Key.StartsWith("sub/"));
    }

    [Fact]
    public async Task AllMethods_WithTopLevelFolder_ShouldWorkCorrectly()
    {
        // Arrange
        using var service = new BlobStorageServiceReadOnly(
            _localStackFixture.S3Client,
            _loggerMock.Object,
            LocalStackFixture.TestBucket,
            TestTopLevelFolder);

        // Test ListAsync
        var items = await service.ListAsync();
        items.Should().HaveCount(2);
        items.Should().OnlyContain(x => !x.Key.StartsWith(TestTopLevelFolder));

        // Test ExistsAsync
        var exists = await service.ExistsAsync("inside-folder.txt");
        exists.Should().BeTrue();

        // Test GetMetadataAsync
        var metadata = await service.GetMetadataAsync("inside-folder.txt");
        metadata.Key.Should().Be("inside-folder.txt");

        // Test DownloadAsync
        var content = await service.DownloadAsync("inside-folder.txt");
        Encoding.UTF8.GetString(content).Should().Be("Content inside top-level folder");

        // Test OpenReadAsync
        using var stream = await service.OpenReadAsync("inside-folder.txt");
        using var reader = new StreamReader(stream);
        var streamContent = await reader.ReadToEndAsync();
        streamContent.Should().Be("Content inside top-level folder");
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    [InlineData("   ")]
    public async Task Constructor_WithEmptyTopLevelFolder_ShouldNotFilterOperations(string? topLevelFolder)
    {
        // Arrange
        using var service = new BlobStorageServiceReadOnly(
            _localStackFi​xture.S3Client,
            _loggerMock.Object,
            LocalStackFixture.TestBucket,
            topLevelFolder);

        // Act
        var items = await service.ListAsync();

        // Assert - Should see all files including those not in any top-level folder
        items.Should().HaveCount(6); // 5 test files + 1 large file
    }
}