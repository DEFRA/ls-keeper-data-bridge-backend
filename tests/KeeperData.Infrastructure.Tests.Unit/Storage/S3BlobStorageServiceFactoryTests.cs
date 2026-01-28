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

public class S3BlobStorageServiceFactoryTests
{
    private readonly Mock<IS3ClientFactory> _s3ClientFactoryMock;
    private readonly Mock<ILoggerFactory> _loggerFactoryMock;
    private readonly StorageConfiguration _storageConfiguration;
    private readonly S3BlobStorageServiceFactory _sut;

    public S3BlobStorageServiceFactoryTests()
    {
        _s3ClientFactoryMock = new Mock<IS3ClientFactory>();
        _loggerFactoryMock = new Mock<ILoggerFactory>();
        _storageConfiguration = new StorageConfiguration
        {
            SourceExternalPrefix = "external-prefix",
            SourceInternalPrefix = "internal-prefix",
            TargetInternalPrefix = "target-prefix"
        };

        // Setup logger factory to return mock loggers
        _loggerFactoryMock.Setup(f => f.CreateLogger(It.IsAny<string>()))
            .Returns(Mock.Of<ILogger>());

        _sut = new S3BlobStorageServiceFactory(
            _s3ClientFactoryMock.Object,
            _loggerFactoryMock.Object,
            _storageConfiguration);
    }

    [Fact]
    public void GetSource_WithExternalType_ReturnsExternalStorageService()
    {
        // Arrange
        var mockS3Client = new Mock<Amazon.S3.IAmazonS3>();
        _s3ClientFactoryMock.Setup(f => f.GetClientInfo<ExternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(mockS3Client.Object, "external-bucket"));

        // Act
        var result = _sut.GetSource(BlobStorageSources.External);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<S3BlobStorageServiceReadOnly>();
        _s3ClientFactoryMock.Verify(f => f.GetClientInfo<ExternalStorageClient>(), Times.Once);
    }

    [Fact]
    public void GetSource_WithInternalType_ReturnsInternalStorageService()
    {
        // Arrange
        var mockS3Client = new Mock<Amazon.S3.IAmazonS3>();
        _s3ClientFactoryMock.Setup(f => f.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(mockS3Client.Object, "internal-bucket"));

        // Act
        var result = _sut.GetSource(BlobStorageSources.Internal);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<S3BlobStorageService>();
        _s3ClientFactoryMock.Verify(f => f.GetClientInfo<InternalStorageClient>(), Times.Once);
    }

    [Fact]
    public void GetSource_WithInvalidType_ThrowsArgumentException()
    {
        // Act
        var act = () => _sut.GetSource("invalid-type");

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*'External'*'Internal'*'invalid-type'*");
    }

    [Fact]
    public void GetSourceExternal_ReturnsReadOnlyService()
    {
        // Arrange
        var mockS3Client = new Mock<Amazon.S3.IAmazonS3>();
        _s3ClientFactoryMock.Setup(f => f.GetClientInfo<ExternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(mockS3Client.Object, "external-bucket"));

        // Act
        var result = _sut.GetSourceExternal();

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<S3BlobStorageServiceReadOnly>();
    }

    [Fact]
    public void GetSourceInternal_ReturnsWritableService()
    {
        // Arrange
        var mockS3Client = new Mock<Amazon.S3.IAmazonS3>();
        _s3ClientFactoryMock.Setup(f => f.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(mockS3Client.Object, "internal-bucket"));

        // Act
        var result = _sut.GetSourceInternal();

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<S3BlobStorageService>();
    }

    [Fact]
    public void Get_ReturnsInternalStorageServiceWithTargetPrefix()
    {
        // Arrange
        var mockS3Client = new Mock<Amazon.S3.IAmazonS3>();
        _s3ClientFactoryMock.Setup(f => f.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(mockS3Client.Object, "internal-bucket"));

        // Act
        var result = _sut.Get();

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<S3BlobStorageService>();
        _s3ClientFactoryMock.Verify(f => f.GetClientInfo<InternalStorageClient>(), Times.Once);
    }

    [Fact]
    public void GetCleanseReportsBlobService_ReturnsServiceWithCleanseReportsPrefix()
    {
        // Arrange
        var mockS3Client = new Mock<Amazon.S3.IAmazonS3>();
        _s3ClientFactoryMock.Setup(f => f.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(mockS3Client.Object, "internal-bucket"));

        // Act
        var result = _sut.GetCleanseReportsBlobService();

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<S3BlobStorageService>();
    }
}
