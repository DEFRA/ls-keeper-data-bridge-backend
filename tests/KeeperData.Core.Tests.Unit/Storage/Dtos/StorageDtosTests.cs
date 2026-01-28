using FluentAssertions;
using KeeperData.Core.Storage.Dtos;

namespace KeeperData.Core.Tests.Unit.Storage.Dtos;

public class StorageObjectInfoTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var storageUri = new Uri("s3://bucket/key");

        var info = new StorageObjectInfo
        {
            Container = "my-bucket",
            Key = "folder/file.csv",
            StorageUri = storageUri
        };

        info.Container.Should().Be("my-bucket");
        info.Key.Should().Be("folder/file.csv");
        info.StorageUri.Should().Be(storageUri);
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var info = new StorageObjectInfo
        {
            Container = "bucket",
            Key = "key",
            StorageUri = new Uri("s3://bucket/key")
        };

        info.Size.Should().Be(0);
        info.LastModified.Should().Be(default);
        info.ETag.Should().BeNull();
        info.HttpUri.Should().BeNull();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var storageUri = new Uri("s3://my-bucket/folder/file.csv");
        var httpUri = new Uri("https://my-bucket.s3.amazonaws.com/folder/file.csv");
        var lastModified = DateTimeOffset.UtcNow;

        var info = new StorageObjectInfo
        {
            Container = "my-bucket",
            Key = "folder/file.csv",
            Size = 1024000,
            LastModified = lastModified,
            ETag = "abc123",
            StorageUri = storageUri,
            HttpUri = httpUri
        };

        info.Container.Should().Be("my-bucket");
        info.Key.Should().Be("folder/file.csv");
        info.Size.Should().Be(1024000);
        info.LastModified.Should().Be(lastModified);
        info.ETag.Should().Be("abc123");
        info.StorageUri.Should().Be(storageUri);
        info.HttpUri.Should().Be(httpUri);
    }

    [Fact]
    public void Record_IsSealed()
    {
        typeof(StorageObjectInfo).IsSealed.Should().BeTrue();
    }

    [Fact]
    public void Record_SupportsEquality()
    {
        var uri = new Uri("s3://bucket/key");
        var info1 = new StorageObjectInfo { Container = "bucket", Key = "key", StorageUri = uri };
        var info2 = new StorageObjectInfo { Container = "bucket", Key = "key", StorageUri = uri };
        var info3 = new StorageObjectInfo { Container = "bucket", Key = "other", StorageUri = uri };

        info1.Should().Be(info2);
        info1.Should().NotBe(info3);
    }
}

public class StorageObjectMetadataTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var storageUri = new Uri("s3://bucket/key");
        var metadata = new Dictionary<string, string> { { "author", "test" } };

        var info = new StorageObjectMetadata
        {
            Container = "my-bucket",
            Key = "folder/file.csv",
            StorageUri = storageUri,
            UserMetadata = metadata
        };

        info.Container.Should().Be("my-bucket");
        info.Key.Should().Be("folder/file.csv");
        info.StorageUri.Should().Be(storageUri);
        info.UserMetadata.Should().ContainKey("author");
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var info = new StorageObjectMetadata
        {
            Container = "bucket",
            Key = "key",
            StorageUri = new Uri("s3://bucket/key"),
            UserMetadata = new Dictionary<string, string>()
        };

        info.ContentLength.Should().Be(0);
        info.ContentType.Should().BeNull();
        info.ETag.Should().BeNull();
        info.LastModified.Should().BeNull();
        info.StorageClass.Should().BeNull();
        info.Encryption.Should().BeNull();
        info.HttpUri.Should().BeNull();
        info.ProviderProperties.Should().BeNull();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var storageUri = new Uri("s3://my-bucket/folder/file.csv");
        var httpUri = new Uri("https://my-bucket.s3.amazonaws.com/folder/file.csv");
        var lastModified = DateTimeOffset.UtcNow;
        var userMetadata = new Dictionary<string, string> { { "author", "test" }, { "version", "1.0" } };
        var providerProps = new Dictionary<string, string> { { "x-amz-storage-class", "STANDARD" } };

        var info = new StorageObjectMetadata
        {
            Container = "my-bucket",
            Key = "folder/file.csv",
            ContentLength = 2048000,
            ContentType = "text/csv",
            ETag = "\"abc123def456\"",
            LastModified = lastModified,
            StorageClass = "STANDARD",
            Encryption = "AES256",
            StorageUri = storageUri,
            HttpUri = httpUri,
            UserMetadata = userMetadata,
            ProviderProperties = providerProps
        };

        info.Container.Should().Be("my-bucket");
        info.Key.Should().Be("folder/file.csv");
        info.ContentLength.Should().Be(2048000);
        info.ContentType.Should().Be("text/csv");
        info.ETag.Should().Be("\"abc123def456\"");
        info.LastModified.Should().Be(lastModified);
        info.StorageClass.Should().Be("STANDARD");
        info.Encryption.Should().Be("AES256");
        info.StorageUri.Should().Be(storageUri);
        info.HttpUri.Should().Be(httpUri);
        info.UserMetadata.Should().HaveCount(2);
        info.ProviderProperties.Should().ContainKey("x-amz-storage-class");
    }

    [Fact]
    public void Record_IsSealed()
    {
        typeof(StorageObjectMetadata).IsSealed.Should().BeTrue();
    }
}

public class StorageListPageTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var items = new List<StorageObjectInfo>
        {
            new() { Container = "bucket", Key = "file1.csv", StorageUri = new Uri("s3://bucket/file1.csv") },
            new() { Container = "bucket", Key = "file2.csv", StorageUri = new Uri("s3://bucket/file2.csv") }
        };

        var page = new StorageListPage { Items = items };

        page.Items.Should().HaveCount(2);
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var page = new StorageListPage { Items = [] };

        page.ContinuationToken.Should().BeNull();
        page.IsTruncated.Should().BeNull();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var items = new List<StorageObjectInfo>
        {
            new() { Container = "bucket", Key = "file1.csv", StorageUri = new Uri("s3://bucket/file1.csv") }
        };

        var page = new StorageListPage
        {
            Items = items,
            ContinuationToken = "abc123token",
            IsTruncated = true
        };

        page.Items.Should().ContainSingle();
        page.ContinuationToken.Should().Be("abc123token");
        page.IsTruncated.Should().BeTrue();
    }

    [Fact]
    public void EmptyPage_CanBeRepresented()
    {
        var page = new StorageListPage
        {
            Items = Array.Empty<StorageObjectInfo>(),
            ContinuationToken = null,
            IsTruncated = false
        };

        page.Items.Should().BeEmpty();
        page.IsTruncated.Should().BeFalse();
    }

    [Fact]
    public void Record_IsSealed()
    {
        typeof(StorageListPage).IsSealed.Should().BeTrue();
    }
}
