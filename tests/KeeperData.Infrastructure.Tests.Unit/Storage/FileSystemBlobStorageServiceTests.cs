using FluentAssertions;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Logging;
using Moq;
using System.Text;

namespace KeeperData.Infrastructure.Tests.Unit.Storage;

public class FileSystemBlobStorageServiceTests : IAsyncLifetime
{
    private readonly string _testBasePath;
    private readonly Mock<ILogger<FileSystemBlobStorageService>> _loggerMock;
    private readonly FileSystemBlobStorageService _sut;

    private const string TestTopLevelFolder = "test-folder";

    public FileSystemBlobStorageServiceTests()
    {
        _testBasePath = Path.Combine(Path.GetTempPath(), $"keeper-fs-tests-{Guid.NewGuid():N}");
        _loggerMock = new Mock<ILogger<FileSystemBlobStorageService>>();
        _sut = new FileSystemBlobStorageService(_loggerMock.Object, _testBasePath, TestTopLevelFolder);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync()
    {
        if (Directory.Exists(_testBasePath))
            Directory.Delete(_testBasePath, recursive: true);

        return Task.CompletedTask;
    }

    #region Upload / Download Round-Trip

    [Fact]
    public async Task UploadAsync_SmallFile_ShouldUploadAndDownloadSuccessfully()
    {
        // Arrange
        var content = Encoding.UTF8.GetBytes("Hello, filesystem!");
        var metadata = new Dictionary<string, string>
        {
            ["test-key"] = "test-value",
            ["source"] = "unit-test"
        };

        // Act
        await _sut.UploadAsync("test.txt", content, "text/plain", metadata);

        // Assert
        var downloaded = await _sut.DownloadAsync("test.txt");
        Encoding.UTF8.GetString(downloaded).Should().Be("Hello, filesystem!");
    }

    [Fact]
    public async Task UploadAsync_BinaryContent_ShouldPreserveExactBytes()
    {
        // Arrange
        var content = new byte[] { 0xFF, 0x00, 0xAB, 0xCD, 0x01, 0x02 };

        // Act
        await _sut.UploadAsync("binary.bin", content, "application/octet-stream");

        // Assert
        var downloaded = await _sut.DownloadAsync("binary.bin");
        downloaded.Should().BeEquivalentTo(content);
    }

    [Fact]
    public async Task UploadAsync_NestedKey_ShouldCreateSubdirectories()
    {
        // Arrange
        var content = Encoding.UTF8.GetBytes("nested content");

        // Act
        await _sut.UploadAsync("level1/level2/file.csv", content, "text/csv");

        // Assert
        var exists = await _sut.ExistsAsync("level1/level2/file.csv");
        exists.Should().BeTrue();

        var downloaded = await _sut.DownloadAsync("level1/level2/file.csv");
        Encoding.UTF8.GetString(downloaded).Should().Be("nested content");
    }

    #endregion

    #region OpenWriteAsync / OpenReadAsync Streaming

    [Fact]
    public async Task OpenWriteAsync_ShouldWriteStreamAndReadBack()
    {
        // Arrange
        var content = Encoding.UTF8.GetBytes("streamed content for write test");
        var metadata = new Dictionary<string, string> { ["stream-key"] = "stream-value" };

        // Act
        await using (var writeStream = await _sut.OpenWriteAsync("streamed.txt", "text/plain", metadata))
        {
            await writeStream.WriteAsync(content);
            await writeStream.FlushAsync();
        }

        // Assert
        await using var readStream = await _sut.OpenReadAsync("streamed.txt");
        using var reader = new StreamReader(readStream);
        var result = await reader.ReadToEndAsync();
        result.Should().Be("streamed content for write test");
    }

    [Fact]
    public async Task OpenWriteAsync_ShouldPersistMetadata()
    {
        // Arrange
        var content = Encoding.UTF8.GetBytes("metadata test");
        var metadata = new Dictionary<string, string> { ["my-key"] = "my-value" };

        // Act
        await using (var writeStream = await _sut.OpenWriteAsync("with-meta.txt", "text/csv", metadata))
        {
            await writeStream.WriteAsync(content);
        }

        // Assert
        var objectMetadata = await _sut.GetMetadataAsync("with-meta.txt");
        objectMetadata.ContentType.Should().Be("text/csv");
        objectMetadata.UserMetadata.Should().ContainKey("my-key").WhoseValue.Should().Be("my-value");
    }

    [Fact]
    public async Task OpenWriteAsync_LargeContent_ShouldHandleCorrectly()
    {
        // Arrange - create content larger than typical buffer sizes
        var largeContent = new byte[1024 * 1024]; // 1MB
        Random.Shared.NextBytes(largeContent);

        // Act
        await using (var writeStream = await _sut.OpenWriteAsync("large.bin", "application/octet-stream"))
        {
            // Write in chunks to simulate streaming
            var chunkSize = 81920;
            for (var offset = 0; offset < largeContent.Length; offset += chunkSize)
            {
                var length = Math.Min(chunkSize, largeContent.Length - offset);
                await writeStream.WriteAsync(largeContent.AsMemory(offset, length));
            }
        }

        // Assert
        var downloaded = await _sut.DownloadAsync("large.bin");
        downloaded.Should().BeEquivalentTo(largeContent);
    }

    #endregion

    #region ExistsAsync

    [Fact]
    public async Task ExistsAsync_ExistingFile_ShouldReturnTrue()
    {
        // Arrange
        await _sut.UploadAsync("exists-test.txt", "data"u8.ToArray());

        // Act & Assert
        (await _sut.ExistsAsync("exists-test.txt")).Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_NonExistentFile_ShouldReturnFalse()
    {
        (await _sut.ExistsAsync("no-such-file.txt")).Should().BeFalse();
    }

    #endregion

    #region GetMetadataAsync

    [Fact]
    public async Task GetMetadataAsync_ShouldReturnCorrectMetadata()
    {
        // Arrange
        var content = Encoding.UTF8.GetBytes("metadata content");
        var metadata = new Dictionary<string, string>
        {
            ["author"] = "test-suite",
            ["version"] = "1"
        };

        await _sut.UploadAsync("meta-test.txt", content, "text/plain", metadata);

        // Act
        var result = await _sut.GetMetadataAsync("meta-test.txt");

        // Assert
        result.Key.Should().Be("meta-test.txt");
        result.ContentLength.Should().Be(content.Length);
        result.ContentType.Should().Be("text/plain");
        result.ETag.Should().NotBeNullOrEmpty();
        result.LastModified.Should().NotBeNull();
        result.UserMetadata.Should().ContainKey("author").WhoseValue.Should().Be("test-suite");
        result.UserMetadata.Should().ContainKey("version").WhoseValue.Should().Be("1");
        result.StorageUri.Should().NotBeNull();
    }

    [Fact]
    public async Task GetMetadataAsync_NonExistentFile_ShouldThrow()
    {
        var act = () => _sut.GetMetadataAsync("nonexistent.txt");
        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task GetMetadataAsync_ETag_ShouldChangeWhenContentChanges()
    {
        // Arrange
        await _sut.UploadAsync("etag-test.txt", "content-v1"u8.ToArray());
        var etag1 = (await _sut.GetMetadataAsync("etag-test.txt")).ETag;

        await _sut.UploadAsync("etag-test.txt", "content-v2"u8.ToArray());
        var etag2 = (await _sut.GetMetadataAsync("etag-test.txt")).ETag;

        // Assert
        etag1.Should().NotBe(etag2);
    }

    [Fact]
    public async Task GetMetadataAsync_ETag_ShouldBeStableForSameContent()
    {
        // Arrange
        var content = "stable-content"u8.ToArray();
        await _sut.UploadAsync("etag-stable.txt", content);
        var etag1 = (await _sut.GetMetadataAsync("etag-stable.txt")).ETag;
        var etag2 = (await _sut.GetMetadataAsync("etag-stable.txt")).ETag;

        // Assert
        etag1.Should().Be(etag2);
    }

    #endregion

    #region SetMetadataAsync

    [Fact]
    public async Task SetMetadataAsync_ShouldUpdateMetadata()
    {
        // Arrange
        var content = "set-meta-content"u8.ToArray();
        await _sut.UploadAsync("set-meta.txt", content, "text/plain",
            new Dictionary<string, string> { ["original"] = "value" });

        // Act
        await _sut.SetMetadataAsync("set-meta.txt",
            new Dictionary<string, string> { ["updated"] = "new-value" });

        // Assert
        var metadata = await _sut.GetMetadataAsync("set-meta.txt");
        metadata.UserMetadata.Should().ContainKey("updated").WhoseValue.Should().Be("new-value");
        metadata.ContentType.Should().Be("text/plain"); // Preserved
    }

    [Fact]
    public async Task SetMetadataAsync_NonExistentFile_ShouldThrow()
    {
        var act = () => _sut.SetMetadataAsync("no-file.txt",
            new Dictionary<string, string> { ["key"] = "value" });

        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    #endregion

    #region DeleteAsync

    [Fact]
    public async Task DeleteAsync_ShouldRemoveFileAndMetadata()
    {
        // Arrange
        await _sut.UploadAsync("to-delete.txt", "content"u8.ToArray(), "text/plain",
            new Dictionary<string, string> { ["key"] = "val" });
        (await _sut.ExistsAsync("to-delete.txt")).Should().BeTrue();

        // Act
        await _sut.DeleteAsync("to-delete.txt");

        // Assert
        (await _sut.ExistsAsync("to-delete.txt")).Should().BeFalse();
    }

    [Fact]
    public async Task DeleteAsync_NonExistentFile_ShouldNotThrow()
    {
        // Idempotent delete — should not throw
        var act = () => _sut.DeleteAsync("does-not-exist.txt");
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task DeleteByPrefixAsync_ShouldDeleteOnlyMatchingFlatKeys()
    {
        await _sut.UploadAsync("LITP_SAMCPHHOLDING_20260819203115.psv", "holding"u8.ToArray());
        await _sut.UploadAsync("LITP_SAMSHOWGROUND_20260819203115.psv", "showground"u8.ToArray());

        var result = await _sut.DeleteByPrefixAsync("LITP_SAMCPHHOLDING_");

        result.DeletedKeys.Should().Equal("LITP_SAMCPHHOLDING_20260819203115.psv");
        (await _sut.ExistsAsync("LITP_SAMCPHHOLDING_20260819203115.psv")).Should().BeFalse();
        (await _sut.ExistsAsync("LITP_SAMSHOWGROUND_20260819203115.psv")).Should().BeTrue();
    }

    #endregion

    #region ClearDownAsync

    [Fact]
    public async Task ClearDownAsync_ShouldDeleteAllFilesUnderTopLevelFolder()
    {
        // Arrange
        await _sut.UploadAsync("file1.txt", "content1"u8.ToArray());
        await _sut.UploadAsync("sub/file2.txt", "content2"u8.ToArray());
        await _sut.UploadAsync("sub/deep/file3.txt", "content3"u8.ToArray());

        // Act
        var result = await _sut.ClearDownAsync();

        // Assert
        result.TotalDeleted.Should().Be(3);
        result.DeletedKeys.Should().HaveCount(3);
        (await _sut.ExistsAsync("file1.txt")).Should().BeFalse();
        (await _sut.ExistsAsync("sub/file2.txt")).Should().BeFalse();
        (await _sut.ExistsAsync("sub/deep/file3.txt")).Should().BeFalse();
    }

    [Fact]
    public async Task ClearDownAsync_EmptyDirectory_ShouldReturnZero()
    {
        var result = await _sut.ClearDownAsync();
        result.TotalDeleted.Should().Be(0);
        result.DeletedKeys.Should().BeEmpty();
    }

    #endregion

    #region ListAsync

    [Fact]
    public async Task ListAsync_ShouldListAllFiles()
    {
        // Arrange
        await _sut.UploadAsync("list/a.txt", "a"u8.ToArray());
        await _sut.UploadAsync("list/b.txt", "b"u8.ToArray());
        await _sut.UploadAsync("other/c.txt", "c"u8.ToArray());

        // Act
        var all = await _sut.ListAsync();

        // Assert
        all.Should().HaveCount(3);
    }

    [Fact]
    public async Task ListAsync_WithPrefix_ShouldFilterByPrefix()
    {
        // Arrange
        await _sut.UploadAsync("prefix-test/a.txt", "a"u8.ToArray());
        await _sut.UploadAsync("prefix-test/b.txt", "b"u8.ToArray());
        await _sut.UploadAsync("other/c.txt", "c"u8.ToArray());

        // Act
        var filtered = await _sut.ListAsync("prefix-test/");

        // Assert
        filtered.Should().HaveCount(2);
        filtered.Should().OnlyContain(f => f.Key.StartsWith("prefix-test/"));
    }

    [Fact]
    public async Task ListAsync_EmptyDirectory_ShouldReturnEmpty()
    {
        var result = await _sut.ListAsync();
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_ShouldReturnCorrectFileInfo()
    {
        // Arrange
        var content = "content for info test"u8.ToArray();
        await _sut.UploadAsync("info-test.txt", content);

        // Act
        var list = await _sut.ListAsync();

        // Assert
        list.Should().ContainSingle();
        var item = list[0];
        item.Key.Should().Be("info-test.txt");
        item.Size.Should().Be(content.Length);
        item.ETag.Should().NotBeNullOrEmpty();
        item.StorageUri.Should().NotBeNull();
    }

    #endregion

    #region ListPageAsync

    [Fact]
    public async Task ListPageAsync_ShouldPaginate()
    {
        // Arrange
        for (var i = 0; i < 5; i++)
            await _sut.UploadAsync($"page-test/file{i}.txt", Encoding.UTF8.GetBytes($"content{i}"));

        // Act - first page
        var page1 = await _sut.ListPageAsync(pageSize: 3);
        page1.Items.Should().HaveCount(3);
        page1.IsTruncated.Should().BeTrue();
        page1.ContinuationToken.Should().NotBeNullOrEmpty();

        // Act - second page
        var page2 = await _sut.ListPageAsync(pageSize: 3, continuationToken: page1.ContinuationToken);
        page2.Items.Should().HaveCount(2);
        page2.IsTruncated.Should().BeFalse();
        page2.ContinuationToken.Should().BeNull();
    }

    [Fact]
    public async Task ListPageAsync_WithPrefix_ShouldFilterCorrectly()
    {
        // Arrange
        await _sut.UploadAsync("scope-a/file1.txt", "a"u8.ToArray());
        await _sut.UploadAsync("scope-a/file2.txt", "b"u8.ToArray());
        await _sut.UploadAsync("scope-b/file3.txt", "c"u8.ToArray());

        // Act
        var page = await _sut.ListPageAsync(prefix: "scope-a/");

        // Assert
        page.Items.Should().HaveCount(2);
        page.Items.Should().OnlyContain(f => f.Key.StartsWith("scope-a/"));
    }

    #endregion

    #region GeneratePresignedUrl

    [Fact]
    public async Task GeneratePresignedUrl_ShouldReturnFileUri()
    {
        // Arrange
        await _sut.UploadAsync("url-test.txt", "data"u8.ToArray());

        // Act
        var url = _sut.GeneratePresignedUrl("url-test.txt");

        // Assert
        url.Should().StartWith("file:///");
        url.Should().Contain("url-test.txt");
    }

    #endregion

    #region Top-Level Folder Isolation

    [Fact]
    public async Task DifferentTopLevelFolders_ShouldBeIsolated()
    {
        // Arrange
        var serviceA = new FileSystemBlobStorageService(_loggerMock.Object, _testBasePath, "folder-a");
        var serviceB = new FileSystemBlobStorageService(_loggerMock.Object, _testBasePath, "folder-b");

        await serviceA.UploadAsync("shared-key.txt", "from-a"u8.ToArray());
        await serviceB.UploadAsync("shared-key.txt", "from-b"u8.ToArray());

        // Assert
        var fromA = await serviceA.DownloadAsync("shared-key.txt");
        var fromB = await serviceB.DownloadAsync("shared-key.txt");

        Encoding.UTF8.GetString(fromA).Should().Be("from-a");
        Encoding.UTF8.GetString(fromB).Should().Be("from-b");

        var listA = await serviceA.ListAsync();
        var listB = await serviceB.ListAsync();

        listA.Should().ContainSingle();
        listB.Should().ContainSingle();
    }

    [Fact]
    public async Task ClearDownAsync_ShouldOnlyAffectOwnTopLevelFolder()
    {
        // Arrange
        var serviceA = new FileSystemBlobStorageService(_loggerMock.Object, _testBasePath, "clear-a");
        var serviceB = new FileSystemBlobStorageService(_loggerMock.Object, _testBasePath, "clear-b");

        await serviceA.UploadAsync("file.txt", "a-content"u8.ToArray());
        await serviceB.UploadAsync("file.txt", "b-content"u8.ToArray());

        // Act
        await serviceA.ClearDownAsync();

        // Assert
        (await serviceA.ExistsAsync("file.txt")).Should().BeFalse();
        (await serviceB.ExistsAsync("file.txt")).Should().BeTrue();
    }

    #endregion

    #region No Top-Level Folder

    [Fact]
    public async Task NoTopLevelFolder_ShouldOperateFromBasePath()
    {
        // Arrange
        var serviceNoFolder = new FileSystemBlobStorageService(_loggerMock.Object, _testBasePath);

        await serviceNoFolder.UploadAsync("root-file.txt", "root-content"u8.ToArray());

        // Assert
        (await serviceNoFolder.ExistsAsync("root-file.txt")).Should().BeTrue();
        var content = await serviceNoFolder.DownloadAsync("root-file.txt");
        Encoding.UTF8.GetString(content).Should().Be("root-content");
    }

    #endregion

    #region Cross-OS Path Handling

    [Fact]
    public async Task ObjectKeyWithForwardSlashes_ShouldWorkCrossOS()
    {
        // Arrange
        var content = "cross-os test"u8.ToArray();

        // Act
        await _sut.UploadAsync("a/b/c/file.txt", content);

        // Assert
        (await _sut.ExistsAsync("a/b/c/file.txt")).Should().BeTrue();
        var downloaded = await _sut.DownloadAsync("a/b/c/file.txt");
        downloaded.Should().BeEquivalentTo(content);

        var list = await _sut.ListAsync("a/b/");
        list.Should().ContainSingle();
        // Keys should use forward slashes regardless of OS
        list[0].Key.Should().Contain("/");
        list[0].Key.Should().NotContain("\\");
    }

    #endregion

    #region ToString

    [Fact]
    public void ToString_ShouldReturnDescriptiveString()
    {
        _sut.ToString().Should().Contain("BlobStorageService(fs=");
        _sut.ToString().Should().Contain(_testBasePath);
    }

    #endregion

    #region DownloadAsync / OpenReadAsync for Non-Existent Files

    [Fact]
    public async Task DownloadAsync_NonExistentFile_ShouldThrow()
    {
        var act = () => _sut.DownloadAsync("no-such-file.bin");
        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task OpenReadAsync_NonExistentFile_ShouldThrow()
    {
        var act = () => _sut.OpenReadAsync("no-such-file.bin");
        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    #endregion
}
