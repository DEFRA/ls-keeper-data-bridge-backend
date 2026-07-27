using FluentAssertions;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Infrastructure.EtlPipeline.Storage;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Storage;

public class S3EtlPipelineStorageTests
{
    private readonly Mock<IS3ClientFactory> _s3ClientFactoryMock;
    private readonly Mock<ILoggerFactory> _loggerFactoryMock;
    private readonly S3EtlPipelineStorage _sut;

    public S3EtlPipelineStorageTests()
    {
        _s3ClientFactoryMock = new Mock<IS3ClientFactory>();
        _loggerFactoryMock = new Mock<ILoggerFactory>();

        _loggerFactoryMock.Setup(f => f.CreateLogger(It.IsAny<string>()))
            .Returns(Mock.Of<ILogger>());

        var mockS3Client = new Mock<Amazon.S3.IAmazonS3>();
        _s3ClientFactoryMock.Setup(f => f.GetClientInfo<InternalStorageClient>())
            .Returns(new StorageClientInfo(mockS3Client.Object, "internal-bucket"));

        _sut = new S3EtlPipelineStorage(_s3ClientFactoryMock.Object, _loggerFactoryMock.Object);
    }

    [Fact]
    public void ForFolder_ReturnsS3BackedService()
    {
        var result = _sut.ForFolder(EtlPipelineFolders.Raw);

        result.Should().NotBeNull();
        result.Should().BeOfType<S3BlobStorageService>();
    }

    [Fact]
    public void ForFolder_ResolvesAgainstTheInternalBucket()
    {
        _sut.ForFolder(EtlPipelineFolders.Raw);

        _s3ClientFactoryMock.Verify(f => f.GetClientInfo<InternalStorageClient>(), Times.Once);
    }

    [Theory]
    [InlineData(EtlPipelineFolders.Raw)]
    [InlineData(EtlPipelineFolders.Normalised)]
    [InlineData(EtlPipelineFolders.Snapshots)]
    [InlineData(EtlPipelineFolders.Staging)]
    [InlineData(EtlPipelineFolders.Views)]
    public void ForFolder_ServesEveryPipelineFolder(string folder)
    {
        var result = _sut.ForFolder(folder);

        result.Should().BeOfType<S3BlobStorageService>();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ForFolder_WithMissingFolder_Throws(string? folder)
    {
        var act = () => _sut.ForFolder(folder!);

        act.Should().Throw<ArgumentException>();
    }
}
