using FluentAssertions;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Infrastructure.EtlPipeline.Storage;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Configuration;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Storage;

public class FileSystemEtlPipelineStorageTests
{
    private readonly Mock<ILoggerFactory> _loggerFactoryMock;

    public FileSystemEtlPipelineStorageTests()
    {
        _loggerFactoryMock = new Mock<ILoggerFactory>();
        _loggerFactoryMock.Setup(f => f.CreateLogger(It.IsAny<string>()))
            .Returns(Mock.Of<ILogger>());
    }

    private static StorageConfiguration CreateConfig(string? basePath = null) => new()
    {
        SourceExternalPrefix = "external-prefix",
        SourceInternalPrefix = "internal-prefix",
        TargetInternalPrefix = "target-prefix",
        UseFileSystem = true,
        FileSystemBasePath = basePath
    };

    private FileSystemEtlPipelineStorage Sut(StorageConfiguration config) =>
        new(_loggerFactoryMock.Object, config);

    [Fact]
    public void ForFolder_ReturnsFileSystemBackedService()
    {
        var result = Sut(CreateConfig(Path.GetTempPath())).ForFolder(EtlPipelineFolders.Raw);

        result.Should().NotBeNull();
        result.Should().BeOfType<FileSystemBlobStorageService>();
    }

    [Fact]
    public void ForFolder_WithoutConfiguredBasePath_StillResolvesAService()
    {
        var result = Sut(CreateConfig(basePath: null)).ForFolder(EtlPipelineFolders.Raw);

        result.Should().BeOfType<FileSystemBlobStorageService>();
    }

    [Theory]
    [InlineData(EtlPipelineFolders.Raw)]
    [InlineData(EtlPipelineFolders.Normalised)]
    [InlineData(EtlPipelineFolders.Snapshots)]
    [InlineData(EtlPipelineFolders.Staging)]
    [InlineData(EtlPipelineFolders.Views)]
    public void ForFolder_ServesEveryPipelineFolder(string folder)
    {
        var result = Sut(CreateConfig(Path.GetTempPath())).ForFolder(folder);

        result.Should().BeOfType<FileSystemBlobStorageService>();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ForFolder_WithMissingFolder_Throws(string? folder)
    {
        var act = () => Sut(CreateConfig(Path.GetTempPath())).ForFolder(folder!);

        act.Should().Throw<ArgumentException>();
    }
}
