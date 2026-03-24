using FluentAssertions;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Storage;

public class FileSystemBlobStorageServiceFactoryTests
{
    private readonly Mock<IS3ClientFactory> _s3ClientFactoryMock;
    private readonly Mock<ILoggerFactory> _loggerFactoryMock;

    public FileSystemBlobStorageServiceFactoryTests()
    {
        _s3ClientFactoryMock = new Mock<IS3ClientFactory>();
        _loggerFactoryMock = new Mock<ILoggerFactory>();

        _loggerFactoryMock.Setup(f => f.CreateLogger(It.IsAny<string>()))
            .Returns(Mock.Of<ILogger>());
    }

    private StorageConfiguration CreateConfig(bool useFileSystem, string? basePath = null)
    {
        return new StorageConfiguration
        {
            SourceExternalPrefix = "external-prefix",
            SourceInternalPrefix = "internal-prefix",
            TargetInternalPrefix = "target-prefix",
            UseFileSystem = useFileSystem,
            FileSystemBasePath = basePath
        };
    }

    #region FileSystemBlobStorageServiceFactory Tests

    [Fact]
    public void Get_ShouldReturnFileSystemBlobStorageService()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: true, basePath: Path.GetTempPath());
        var sut = new FileSystemBlobStorageServiceFactory(_s3ClientFactoryMock.Object, _loggerFactoryMock.Object, config);

        // Act
        var result = sut.Get();

        // Assert
        result.Should().BeOfType<FileSystemBlobStorageService>();
    }

    [Fact]
    public void GetSourceInternal_ShouldReturnFileSystemBlobStorageService()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: true, basePath: Path.GetTempPath());
        var sut = new FileSystemBlobStorageServiceFactory(_s3ClientFactoryMock.Object, _loggerFactoryMock.Object, config);

        // Act
        var result = sut.GetSourceInternal();

        // Assert
        result.Should().BeOfType<FileSystemBlobStorageService>();
    }

    [Fact]
    public void GetCleanseReportsBlobService_ShouldReturnFileSystemBlobStorageService()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: true, basePath: Path.GetTempPath());
        var sut = new FileSystemBlobStorageServiceFactory(_s3ClientFactoryMock.Object, _loggerFactoryMock.Object, config);

        // Act
        var result = sut.GetCleanseReportsBlobService();

        // Assert
        result.Should().BeOfType<FileSystemBlobStorageService>();
    }

    [Fact]
    public void GetSourceExternal_ShouldAlwaysReturnS3Service()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: true, basePath: Path.GetTempPath());
        var sut = new FileSystemBlobStorageServiceFactory(_s3ClientFactoryMock.Object, _loggerFactoryMock.Object, config);

        var mockS3Client = new Mock<Amazon.S3.IAmazonS3>();
        _s3ClientFactoryMock.Setup(f => f.GetClientInfo<ExternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(mockS3Client.Object, "external-bucket"));

        // Act
        var result = sut.GetSourceExternal();

        // Assert
        result.Should().BeOfType<S3BlobStorageServiceReadOnly>();
    }

    [Fact]
    public void GetSource_WithExternalType_ShouldReturnS3Service()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: true, basePath: Path.GetTempPath());
        var sut = new FileSystemBlobStorageServiceFactory(_s3ClientFactoryMock.Object, _loggerFactoryMock.Object, config);

        var mockS3Client = new Mock<Amazon.S3.IAmazonS3>();
        _s3ClientFactoryMock.Setup(f => f.GetClientInfo<ExternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(mockS3Client.Object, "external-bucket"));

        // Act
        var result = sut.GetSource(BlobStorageSources.External);

        // Assert
        result.Should().BeOfType<S3BlobStorageServiceReadOnly>();
    }

    [Fact]
    public void GetSource_WithInternalType_ShouldReturnFileSystemService()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: true, basePath: Path.GetTempPath());
        var sut = new FileSystemBlobStorageServiceFactory(_s3ClientFactoryMock.Object, _loggerFactoryMock.Object, config);

        // Act
        var result = sut.GetSource(BlobStorageSources.Internal);

        // Assert
        result.Should().BeOfType<FileSystemBlobStorageService>();
    }

    [Fact]
    public void GetSource_WithInvalidType_ShouldThrowArgumentException()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: true, basePath: Path.GetTempPath());
        var sut = new FileSystemBlobStorageServiceFactory(_s3ClientFactoryMock.Object, _loggerFactoryMock.Object, config);

        // Act
        var act = () => sut.GetSource("invalid-type");

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*'External'*'Internal'*'invalid-type'*");
    }

    [Fact]
    public void WhenNoBasePathConfigured_ShouldUseTempDirectory()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: true, basePath: null);
        var sut = new FileSystemBlobStorageServiceFactory(_s3ClientFactoryMock.Object, _loggerFactoryMock.Object, config);

        // Act
        var result = sut.Get();

        // Assert
        result.Should().BeOfType<FileSystemBlobStorageService>();
        result.ToString().Should().Contain("keeper-data-bridge");
    }

    #endregion

    #region S3BlobStorageServiceFactory (default) Tests

    [Fact]
    public void S3Factory_Get_ShouldReturnS3BlobStorageService()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: false);
        var mockS3Client = new Mock<Amazon.S3.IAmazonS3>();
        _s3ClientFactoryMock.Setup(f => f.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(mockS3Client.Object, "internal-bucket"));

        var sut = new S3BlobStorageServiceFactory(_s3ClientFactoryMock.Object, _loggerFactoryMock.Object, config);

        // Act
        var result = sut.Get();

        // Assert
        result.Should().BeOfType<S3BlobStorageService>();
    }

    [Fact]
    public void S3Factory_GetSourceInternal_ShouldReturnS3BlobStorageService()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: false);
        var mockS3Client = new Mock<Amazon.S3.IAmazonS3>();
        _s3ClientFactoryMock.Setup(f => f.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(mockS3Client.Object, "internal-bucket"));

        var sut = new S3BlobStorageServiceFactory(_s3ClientFactoryMock.Object, _loggerFactoryMock.Object, config);

        // Act
        var result = sut.GetSourceInternal();

        // Assert
        result.Should().BeOfType<S3BlobStorageService>();
    }

    #endregion
}
