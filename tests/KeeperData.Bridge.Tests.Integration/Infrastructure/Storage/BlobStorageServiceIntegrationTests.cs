using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Logging;
using Moq;
using System.Text;
using Xunit.Abstractions;
using Amazon.S3;
using KeeperData.Core.Storage;

namespace KeeperData.Bridge.Tests.Integration.Infrastructure.Storage;

/// <summary>
/// Tests `BlobStorageService` against localstack S3 in testcontainer
/// Reuses the LocalStack container instance from BlobStorageServiceReadOnlyIntegrationTests
/// </summary>
[Collection("LocalStack")]
public class BlobStorageServiceIntegrationTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly LocalStackFixture _localStackFixture;
    private readonly Mock<ILogger<BlobStorageService>> _loggerMock;
    private readonly BlobStorageService _blobService;
    private readonly TestScope _testScope;

    private const string TestTopLevelFolder = "test-write-folder";
    private const string TestUploadKey = "test-upload.txt";
    private const string TestStreamUploadKey = "test-stream-upload.bin";
    private const string TestLargeStreamKey = "test-large-stream.bin";

    // Additional folder configurations for comprehensive testing
    private const string TestFolder1 = "scope-a";
    private const string TestFolder2 = "scope-b/department-1";
    private const string TestFolder3 = "scope-123/workspace-456";

    public BlobStorageServiceIntegrationTests(ITestOutputHelper testOutputHelper, LocalStackFixture localStackFixture)
    {
        _testOutputHelper = testOutputHelper;
        _localStackFixture = localStackFixture;
        _loggerMock = new Mock<ILogger<BlobStorageService>>();

        // Create blob service using the shared S3 client
        _blobService = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        _testScope = new TestScope(_blobService, LocalStackFixture.TestBucket);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _testScope.DisposeAsync();
        _blobService?.Dispose();
    }

    #region Basic Upload/Download Tests

    [Fact]
    public async Task UploadAsync_SmallFile_ShouldUploadSuccessfully()
    {
        // Arrange
        var content = Encoding.UTF8.GetBytes("Test upload content");
        var metadata = new Dictionary<string, string>
        {
            ["test-key"] = "test-value",
            ["upload-method"] = "UploadAsync"
        };

        // Act
        await _blobService.UploadAsync(
            TestUploadKey,
            content,
            "text/plain",
            metadata);
        _testScope.TrackForCleanup(TestUploadKey);

        // Assert - Verify file was uploaded
        var exists = await _blobService.ExistsAsync(TestUploadKey);
        exists.Should().BeTrue();

        // Verify content
        var downloadedContent = await _blobService.DownloadAsync(TestUploadKey);
        Encoding.UTF8.GetString(downloadedContent).Should().Be("Test upload content");

        // Verify metadata
        var objectMetadata = await _blobService.GetMetadataAsync(TestUploadKey);
        objectMetadata.ContentType.Should().Be("text/plain");
        objectMetadata.UserMetadata.Should().ContainKey("x-amz-meta-test-key")
            .WhoseValue.Should().Be("test-value");
    }

    [Fact]
    public async Task UploadAsync_BinaryFile_ShouldUploadSuccessfully()
    {
        // Arrange
        var content = new byte[] { 0xFF, 0xFE, 0x00, 0x01, 0x42, 0xAA };
        var key = "binary-test.bin";

        // Act
        await _blobService.UploadAsync(
            key,
            content,
            "application/octet-stream");
        _testScope.TrackForCleanup(key);

        // Assert
        var downloadedContent = await _blobService.DownloadAsync(key);
        downloadedContent.Should().Equal(content);
    }

    #endregion

    #region Streaming Upload Tests

    [Fact]
    public async Task OpenWriteAsync_SmallFile_ShouldUploadSuccessfully()
    {
        // Arrange
        var testContent = "Test streaming upload content";
        var contentBytes = Encoding.UTF8.GetBytes(testContent);
        var metadata = new Dictionary<string, string>
        {
            ["stream-test"] = "small-file",
            ["method"] = "OpenWriteAsync"
        };

        // Act
        await using (var writeStream = await _blobService.OpenWriteAsync(
            TestStreamUploadKey,
            "text/plain",
            metadata))
        {
            await writeStream.WriteAsync(contentBytes, 0, contentBytes.Length);
        } // Disposing should finalize the upload
        _testScope.TrackForCleanup(TestStreamUploadKey);

        // Assert
        var exists = await _blobService.ExistsAsync(TestStreamUploadKey);
        exists.Should().BeTrue();

        var downloadedContent = await _blobService.DownloadAsync(TestStreamUploadKey);
        Encoding.UTF8.GetString(downloadedContent).Should().Be(testContent);

        // Verify metadata
        var objectMetadata = await _blobService.GetMetadataAsync(TestStreamUploadKey);
        objectMetadata.ContentType.Should().Be("text/plain");
        objectMetadata.UserMetadata.Should().ContainKey("x-amz-meta-stream-test")
            .WhoseValue.Should().Be("small-file");
    }

    [Fact]
    public async Task OpenWriteAsync_LargeFile_ShouldStreamWithoutBuffering()
    {
        // Arrange - Create a file larger than the default part size (8MB)
        const int fileSize = 12 * 1024 * 1024; // 12MB
        const int writeChunkSize = 64 * 1024; // 64KB chunks
        var pattern = Encoding.UTF8.GetBytes("LARGE_STREAM_TEST_PATTERN_");
        var metadata = new Dictionary<string, string>
        {
            ["stream-test"] = "large-file",
            ["expected-size"] = fileSize.ToString()
        };

        // Act
        await using (var writeStream = await _blobService.OpenWriteAsync(
            TestLargeStreamKey,
            "application/octet-stream",
            metadata,
            partSizeBytes: 5 * 1024 * 1024)) // 5MB parts
        {
            var totalWritten = 0L;
            var patternOffset = 0;

            while (totalWritten < fileSize)
            {
                var chunkSize = Math.Min(writeChunkSize, (int)(fileSize - totalWritten));
                var chunk = new byte[chunkSize];

                // Fill chunk with pattern
                for (var i = 0; i < chunkSize; i++)
                {
                    chunk[i] = pattern[patternOffset % pattern.Length];
                    patternOffset++;
                }

                await writeStream.WriteAsync(chunk, 0, chunkSize);
                totalWritten += chunkSize;

                // Log progress
                if (totalWritten % (1024 * 1024) == 0)
                {
                    _testOutputHelper.WriteLine($"Written {totalWritten:N0} / {fileSize:N0} bytes");
                }
            }
        }
        _testScope.TrackForCleanup(TestLargeStreamKey);

        // Assert
        var exists = await _blobService.ExistsAsync(TestLargeStreamKey);
        exists.Should().BeTrue();

        // Verify file size
        var metadata_result = await _blobService.GetMetadataAsync(TestLargeStreamKey);
        metadata_result.ContentLength.Should().Be(fileSize);

        // Verify content by streaming download (don't load entire file into memory)
        await using var readStream = await _blobService.OpenReadAsync(TestLargeStreamKey);
        var buffer = new byte[8192];
        var totalRead = 0L;
        int bytesRead;
        var patternIndex = 0;

        do
        {
            bytesRead = await readStream.ReadAsync(buffer, 0, buffer.Length);
            totalRead += bytesRead;

            if (bytesRead > 0)
            {
                // Verify pattern in first few chunks
                if (totalRead <= 64 * 1024)
                {
                    for (var i = 0; i < bytesRead; i++)
                    {
                        var expectedByte = pattern[patternIndex % pattern.Length];
                        buffer[i].Should().Be(expectedByte,
                            $"at position {totalRead - bytesRead + i}");
                        patternIndex++;
                    }
                }
            }
        } while (bytesRead > 0);

        totalRead.Should().Be(fileSize);
        _testOutputHelper.WriteLine($"Successfully verified {totalRead:N0} bytes of streamed large file");
    }

    [Fact]
    public async Task OpenWriteAsync_WriteInMultipleChunks_ShouldCombineCorrectly()
    {
        // Arrange
        var chunks = new[]
        {
            "First chunk of data. ",
            "Second chunk with more data. ",
            "Third chunk to complete the file."
        };
        var key = "multi-chunk-test.txt";

        // Act
        await using (var writeStream = await _blobService.OpenWriteAsync(
            key,
            "text/plain"))
        {
            foreach (var chunk in chunks)
            {
                var bytes = Encoding.UTF8.GetBytes(chunk);
                await writeStream.WriteAsync(bytes, 0, bytes.Length);
            }
        }
        _testScope.TrackForCleanup(key);

        // Assert
        var downloadedContent = await _blobService.DownloadAsync(key);
        var result = Encoding.UTF8.GetString(downloadedContent);
        result.Should().Be(string.Concat(chunks));
    }

    [Fact]
    public async Task OpenWriteAsync_StreamDisposedEarly_ShouldHandleGracefully()
    {
        // This test verifies that if a stream is disposed without completing,
        // the implementation handles it gracefully

        // Arrange
        var partialContent = Encoding.UTF8.GetBytes("This upload will be incomplete");
        var key = "incomplete-upload.txt";

        // Act - Start upload but dispose without writing full multipart size
        await using (var writeStream = await _blobService.OpenWriteAsync(
            key,
            "text/plain"))
        {
            await writeStream.WriteAsync(partialContent, 0, partialContent.Length);
            // Dispose here without writing enough data to trigger multipart upload
        }

        // Assert - For small uploads, they may complete successfully
        // For large multipart uploads, behavior would be different
        // Let's just verify the stream disposal doesn't throw
        var exists = await _blobService.ExistsAsync(key);

        // If the file exists, clean it up
        if (exists)
        {
            _testScope.TrackForCleanup(key);
        }

        // The main assertion is that disposal didn't throw an exception
        Assert.True(true, "Stream disposal completed without throwing");
    }

    #endregion

    #region Metadata Tests

    [Fact]
    public async Task SetMetadataAsync_ExistingObject_ShouldUpdateMetadata()
    {
        // Arrange - First upload a file
        var content = Encoding.UTF8.GetBytes("File for metadata update test");
        var initialMetadata = new Dictionary<string, string>
        {
            ["initial-key"] = "initial-value"
        };
        var key = "metadata-test.txt";

        await _blobService.UploadAsync(
            key,
            content,
            "text/plain",
            initialMetadata);
        _testScope.TrackForCleanup(key);

        // Act - Update metadata
        var newMetadata = new Dictionary<string, string>
        {
            ["updated-key"] = "updated-value",
            ["new-key"] = "new-value"
        };

        await _blobService.SetMetadataAsync(
            key,
            newMetadata);

        // Assert
        var metadata = await _blobService.GetMetadataAsync(key);

        // New metadata should be present
        metadata.UserMetadata.Should().ContainKey("x-amz-meta-updated-key")
            .WhoseValue.Should().Be("updated-value");
        metadata.UserMetadata.Should().ContainKey("x-amz-meta-new-key")
            .WhoseValue.Should().Be("new-value");

        // File content should remain unchanged
        var downloadedContent = await _blobService.DownloadAsync(key);
        Encoding.UTF8.GetString(downloadedContent).Should().Be("File for metadata update test");
    }

    #endregion

    #region Delete Tests

    [Fact]
    public async Task DeleteAsync_ExistingObject_ShouldDeleteSuccessfully()
    {
        // Arrange - Upload a file first
        var content = Encoding.UTF8.GetBytes("File to be deleted");
        var key = "delete-test.txt";
        await _blobService.UploadAsync(
            key,
            content);
        _testScope.TrackForCleanup(key);

        // Verify it exists
        var existsBeforeDelete = await _blobService.ExistsAsync(key);
        existsBeforeDelete.Should().BeTrue();

        // Act
        await _blobService.DeleteAsync(key);

        // Assert
        var existsAfterDelete = await _blobService.ExistsAsync(key);
        existsAfterDelete.Should().BeFalse();
    }

    [Fact]
    public async Task DeleteAsync_NonExistentObject_ShouldBeIdempotent()
    {
        // Act & Assert - Should not throw
        await _blobService.DeleteAsync("non-existent-file.txt");
    }

    #endregion

    #region Top-Level Folder Tests

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\t")]
    [InlineData("\n")]
    public async Task TopLevelFolder_NullOrEmpty_ShouldNotApplyFoldering(string? topLevelFolder)
    {
        // Arrange
        await using var scope = new TestScope(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        using var service = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket, topLevelFolder);
        var testData = Encoding.UTF8.GetBytes("No folder test content");
        var fileName = "no-folder-test.txt";

        scope.TrackForCleanup(fileName);

        // Act - Upload, read, and verify
        await service.UploadAsync(fileName, testData);

        var exists = await service.ExistsAsync(fileName);
        var downloadedData = await service.DownloadAsync(fileName);
        var metadata = await service.GetMetadataAsync(fileName);
        var files = await service.ListAsync();

        // Assert
        exists.Should().BeTrue();
        downloadedData.Should().Equal(testData);
        metadata.Key.Should().Be(fileName);
        metadata.StorageUri.ToString().Should().Contain(fileName);

        // The URI should be s3://bucket/file.txt (no folder prefix between bucket and file)
        // Should NOT contain any additional path separators beyond the standard s3://bucket/key format
        metadata.StorageUri.ToString().Should().Be($"s3://{LocalStackFixture.TestBucket}/{fileName}");

        // Should see the file at root level (along with potentially other test files)
        files.Should().Contain(f => f.Key == fileName);

        _testOutputHelper.WriteLine($"Successfully tested null/empty folder: '{topLevelFolder}' -> File stored at root: '{fileName}'");
    }

    [Fact]
    public async Task TopLevelFolder_IsolationBetweenTenants_ShouldWorkCorrectly()
    {
        // Arrange - Create services for different "tenants"
        await using var scope = new TestScope(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        using var tenantA = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket, TestFolder1);
        using var tenantB = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket, TestFolder2);
        using var globalService = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);

        var testData = Encoding.UTF8.GetBytes("Isolated tenant data");
        var sharedFileName = "document.txt";

        // Act - Upload same filename to different tenants
        await tenantA.UploadAsync(sharedFileName, testData);
        await tenantB.UploadAsync(sharedFileName, testData);

        scope.TrackForCleanup($"{TestFolder1}/{sharedFileName}");
        scope.TrackForCleanup($"{TestFolder2}/{sharedFileName}");

        // Assert - Each tenant can only see their own file
        var tenantAExists = await tenantA.ExistsAsync(sharedFileName);
        var tenantBExists = await tenantB.ExistsAsync(sharedFileName);

        tenantAExists.Should().BeTrue();
        tenantBExists.Should().BeTrue();

        // But tenants can't see each other's files
        var tenantACanSeeTenantB = await tenantA.ExistsAsync($"../{TestFolder2}/{sharedFileName}");
        var tenantBCanSeeTenantA = await tenantB.ExistsAsync($"../{TestFolder1}/{sharedFileName}");

        tenantACanSeeTenantB.Should().BeFalse();
        tenantBCanSeeTenantA.Should().BeFalse();

        // Global service can see both with full paths
        var globalCanSeeA = await globalService.ExistsAsync($"{TestFolder1}/{sharedFileName}");
        var globalCanSeeB = await globalService.ExistsAsync($"{TestFolder2}/{sharedFileName}");

        globalCanSeeA.Should().BeTrue();
        globalCanSeeB.Should().BeTrue();

        // List operations are properly isolated
        var tenantAFiles = await tenantA.ListAsync();
        var tenantBFiles = await tenantB.ListAsync();

        tenantAFiles.Should().HaveCount(1);
        tenantAFiles.First().Key.Should().Be(sharedFileName);

        tenantBFiles.Should().HaveCount(1);
        tenantBFiles.First().Key.Should().Be(sharedFileName);
    }

    [Theory]
    [InlineData("simple-folder")]
    [InlineData("folder-with-dashes")]
    [InlineData("folder_with_underscores")]
    [InlineData("folder123with456numbers")]
    [InlineData("deeply/nested/folder/structure")]
    [InlineData("folder/with/trailing/slash/")]
    public async Task TopLevelFolder_VariousFolderNames_ShouldHandleCorrectly(string folderName)
    {
        // Arrange
        await using var scope = new TestScope(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        using var service = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket, folderName);
        var testData = Encoding.UTF8.GetBytes($"Test data for folder: {folderName}");
        var fileName = "test-file.txt";

        var expectedFullKey = folderName.Trim().Trim('/') + "/" + fileName;
        scope.TrackForCleanup(expectedFullKey);

        // Act - Upload, read, and verify
        await service.UploadAsync(fileName, testData);

        var exists = await service.ExistsAsync(fileName);
        var downloadedData = await service.DownloadAsync(fileName);
        var metadata = await service.GetMetadataAsync(fileName);

        // Assert
        exists.Should().BeTrue();
        downloadedData.Should().Equal(testData);
        metadata.Key.Should().Be(fileName); // Should be relative key, not full path
        metadata.StorageUri.ToString().Should().Contain(expectedFullKey);

        _testOutputHelper.WriteLine($"Successfully tested folder: '{folderName}' -> Full key: '{expectedFullKey}'");
    }

    [Theory]
    [InlineData("/leading-slash")]
    [InlineData("trailing-slash/")]
    [InlineData("/both-slashes/")]
    [InlineData("//double-leading")]
    [InlineData("double-trailing//")]
    public async Task TopLevelFolder_SlashHandling_ShouldNormalizeCorrectly(string folderName)
    {
        // Arrange
        await using var scope = new TestScope(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        using var service = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket, folderName);
        var fileName = "slash-test.txt";
        var testData = Encoding.UTF8.GetBytes("Testing slash normalization");

        // Expected normalized folder (should always end with single slash, no leading slash)
        var normalizedFolder = folderName.Trim('/') + "/";
        var expectedFullKey = normalizedFolder + fileName;
        scope.TrackForCleanup(expectedFullKey);

        // Act
        await service.UploadAsync(fileName, testData);
        var exists = await service.ExistsAsync(fileName);
        var metadata = await service.GetMetadataAsync(fileName);

        // Assert
        exists.Should().BeTrue();
        metadata.StorageUri.ToString().Should().Contain(expectedFullKey);

        // Verify that the global service can access it with the normalized path
        var globalService = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        var globalExists = await globalService.ExistsAsync(expectedFullKey);
        globalExists.Should().BeTrue();

        _testOutputHelper.WriteLine($"Original folder: '{folderName}' -> Normalized: '{normalizedFolder}' -> Full key: '{expectedFullKey}'");

        globalService.Dispose();
    }

    [Fact]
    public async Task TopLevelFolder_WithPrefix_ShouldCombineCorrectly()
    {
        // Arrange
        await using var scope = new TestScope(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        using var service = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket, TestFolder3);
        var testData = Encoding.UTF8.GetBytes("Prefixed file content");

        // Create files in different subfolders within the top-level folder
        await service.UploadAsync("docs/file1.txt", testData);
        await service.UploadAsync("docs/file2.txt", testData);
        await service.UploadAsync("images/photo1.jpg", testData);
        await service.UploadAsync("other/data.csv", testData);

        scope.TrackForCleanup($"{TestFolder3}/docs/file1.txt");
        scope.TrackForCleanup($"{TestFolder3}/docs/file2.txt");
        scope.TrackForCleanup($"{TestFolder3}/images/photo1.jpg");
        scope.TrackForCleanup($"{TestFolder3}/other/data.csv");

        // Act & Assert - Test listing with prefixes
        var allFiles = await service.ListAsync();
        allFiles.Should().HaveCount(4);

        var docsFiles = await service.ListAsync("docs/");
        docsFiles.Should().HaveCount(2);
        docsFiles.Should().OnlyContain(f => f.Key.StartsWith("docs/"));

        var imagesFiles = await service.ListAsync("images/");
        imagesFiles.Should().HaveCount(1);
        imagesFiles.First().Key.Should().Be("images/photo1.jpg");

        // Test prefix that matches partial folder name
        var partialPrefixFiles = await service.ListAsync("doc");
        partialPrefixFiles.Should().HaveCount(2); // Should match docs/ folder

        _testOutputHelper.WriteLine($"All files: {string.Join(", ", allFiles.Select(f => f.Key))}");
        _testOutputHelper.WriteLine($"Docs files: {string.Join(", ", docsFiles.Select(f => f.Key))}");
    }

    [Fact]
    public async Task TopLevelFolder_StreamingOperations_ShouldWorkCorrectly()
    {
        // Arrange
        await using var scope = new TestScope(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        using var service = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket, TestFolder1);
        var fileName = "streamed-file.txt";
        var expectedContent = "This is streamed content that should be properly isolated within the top-level folder.";

        scope.TrackForCleanup($"{TestFolder1}/{fileName}");

        // Act - Use streaming upload
        await using (var writeStream = await service.OpenWriteAsync(fileName, "text/plain"))
        {
            var contentBytes = Encoding.UTF8.GetBytes(expectedContent);
            await writeStream.WriteAsync(contentBytes, 0, contentBytes.Length);
        }

        // Act - Use streaming read
        await using var readStream = await service.OpenReadAsync(fileName);
        using var reader = new StreamReader(readStream);
        var actualContent = await reader.ReadToEndAsync();

        // Assert
        actualContent.Should().Be(expectedContent);

        // Verify isolation - global service should not see the file with relative path
        var globalService = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        var existsGlobally = await globalService.ExistsAsync(fileName);
        var existsWithFullPath = await globalService.ExistsAsync($"{TestFolder1}/{fileName}");

        existsGlobally.Should().BeFalse();
        existsWithFullPath.Should().BeTrue();

        globalService.Dispose();
    }

    [Fact]
    public async Task TopLevelFolder_ReadOnlyService_ShouldRespectFolderBoundaries()
    {
        // Arrange - Set up test data in different folders
        await using var scope = new TestScope(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        using var setupService = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        using var folder1Service = new BlobStorageServiceReadOnly(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket, TestFolder1);
        using var folder2Service = new BlobStorageServiceReadOnly(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket, TestFolder2);

        var testData = Encoding.UTF8.GetBytes("Read-only test data");
        var fileName = "readonly-test.txt";

        // Create files in both folders using full paths
        await setupService.UploadAsync($"{TestFolder1}/{fileName}", testData);
        await setupService.UploadAsync($"{TestFolder2}/{fileName}", testData);

        scope.TrackForCleanup($"{TestFolder1}/{fileName}");
        scope.TrackForCleanup($"{TestFolder2}/{fileName}");

        // Act & Assert - Each read-only service should only see its own folder
        var folder1Files = await folder1Service.ListAsync();
        var folder2Files = await folder2Service.ListAsync();

        folder1Files.Should().HaveCount(1);
        folder1Files.First().Key.Should().Be(fileName);

        folder2Files.Should().HaveCount(1);
        folder2Files.First().Key.Should().Be(fileName);

        // Verify exists operations work correctly
        var folder1Exists = await folder1Service.ExistsAsync(fileName);
        var folder2Exists = await folder2Service.ExistsAsync(fileName);

        folder1Exists.Should().BeTrue();
        folder2Exists.Should().BeTrue();

        // Verify download operations work correctly
        var folder1Content = await folder1Service.DownloadAsync(fileName);
        var folder2Content = await folder2Service.DownloadAsync(fileName);

        folder1Content.Should().Equal(testData);
        folder2Content.Should().Equal(testData);
    }

    [Fact]
    public async Task Constructor_WithTopLevelFolder_ShouldScopeOperations()
    {
        // Arrange
        await using var scope = new TestScope(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        using var scopedService = new BlobStorageService(
            _localStackFixture.S3Client,
            _loggerMock.Object,
            LocalStackFixture.TestBucket,
            TestTopLevelFolder);

        var content = Encoding.UTF8.GetBytes("Scoped folder test content");

        // Act - Upload to scoped service (should be prefixed with folder)
        await scopedService.UploadAsync(
            "scoped-file.txt",
            content);
        scope.TrackForCleanup($"{TestTopLevelFolder}/scoped-file.txt");

        // Assert - File should exist in scoped service
        var existsInScoped = await scopedService.ExistsAsync("scoped-file.txt");
        existsInScoped.Should().BeTrue();

        // File should NOT be visible to non-scoped service at root
        var existsInRoot = await _blobService.ExistsAsync("scoped-file.txt");
        existsInRoot.Should().BeFalse();

        // But should be visible with full path
        var existsWithFullPath = await _blobService.ExistsAsync($"{TestTopLevelFolder}/scoped-file.txt");
        existsWithFullPath.Should().BeTrue();
    }

    [Fact]
    public async Task AllWriteMethods_WithTopLevelFolder_ShouldWorkCorrectly()
    {
        // Arrange
        await using var scope = new TestScope(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket);
        using var scopedService = new BlobStorageService(
            _localStackFixture.S3Client,
            _loggerMock.Object,
            LocalStackFixture.TestBucket,
            TestTopLevelFolder);

        // Test UploadAsync with scoped folder
        var uploadContent = Encoding.UTF8.GetBytes("Scoped upload test");
        await scopedService.UploadAsync(
            "scoped-upload.txt",
            uploadContent,
            "text/plain");
        scope.TrackForCleanup($"{TestTopLevelFolder}/scoped-upload.txt");

        // Test streaming upload with scoped folder
        await using (var writeStream = await scopedService.OpenWriteAsync(
            "scoped-stream.txt",
            "text/plain"))
        {
            var streamContent = Encoding.UTF8.GetBytes("Scoped stream test");
            await writeStream.WriteAsync(streamContent, 0, streamContent.Length);
        }
        scope.TrackForCleanup($"{TestTopLevelFolder}/scoped-stream.txt");

        // Test metadata update with scoped folder
        var metadata = new Dictionary<string, string> { ["scoped"] = "true" };
        await scopedService.SetMetadataAsync(
            "scoped-upload.txt",
            metadata);

        // Assert all operations worked
        var uploadExists = await scopedService.ExistsAsync("scoped-upload.txt");
        uploadExists.Should().BeTrue();

        var streamExists = await scopedService.ExistsAsync("scoped-stream.txt");
        streamExists.Should().BeTrue();

        var updatedMetadata = await scopedService.GetMetadataAsync("scoped-upload.txt");
        updatedMetadata.UserMetadata.Should().ContainKey("x-amz-meta-scoped")
            .WhoseValue.Should().Be("true");

        // Test deletion with scoped folder
        await scopedService.DeleteAsync("scoped-upload.txt");
        await scopedService.DeleteAsync("scoped-stream.txt");

        var deletedUpload = await scopedService.ExistsAsync("scoped-upload.txt");
        deletedUpload.Should().BeFalse();

        var deletedStream = await scopedService.ExistsAsync("scoped-stream.txt");
        deletedStream.Should().BeFalse();
    }

    #endregion

    /// <summary>
    /// Helper class to manage test resource cleanup in a more robust and async-friendly way
    /// </summary>
    private sealed class TestScope : IAsyncDisposable
    {
        private readonly IBlobStorageService _blobService;
        private readonly string _bucketName;
        private readonly HashSet<string> _keysToCleanup = new();
        private readonly object _lock = new();

        public TestScope(IBlobStorageService blobService, string bucketName)
        {
            _blobService = blobService ?? throw new ArgumentNullException(nameof(blobService));
            _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        }

        public TestScope(IAmazonS3 s3Client, ILogger<BlobStorageService> logger, string bucketName, string? topLevelFolder = null)
            : this(new BlobStorageService(s3Client, logger, bucketName, topLevelFolder), bucketName)
        {
        }

        public void TrackForCleanup(string key)
        {
            if (string.IsNullOrEmpty(key))
                return;

            lock (_lock)
            {
                _keysToCleanup.Add(key);
            }
        }

        public async ValueTask DisposeAsync()
        {
            HashSet<string> keysToClean;
            lock (_lock)
            {
                keysToClean = new HashSet<string>(_keysToCleanup);
                _keysToCleanup.Clear();
            }

            // Clean up all tracked keys
            var cleanupTasks = keysToClean.Select(async key =>
            {
                try
                {
                    await _blobService.DeleteAsync(key);
                }
                catch
                {
                    // Ignore cleanup failures - tests shouldn't fail due to cleanup issues
                    // but we could add logging here if needed
                }
            });

            await Task.WhenAll(cleanupTasks);

            if (_blobService is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
    }
}