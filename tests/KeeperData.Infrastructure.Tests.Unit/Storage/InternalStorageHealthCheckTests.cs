using FluentAssertions;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.Setup;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KeeperData.Infrastructure.Tests.Unit.Storage;

public class InternalStorageHealthCheckTests
{
    private static StorageConfiguration CreateConfig(bool useFileSystem, string? basePath = null)
    {
        return new StorageConfiguration
        {
            SourceExternalPrefix = "external",
            SourceInternalPrefix = "internal",
            TargetInternalPrefix = "target",
            UseFileSystem = useFileSystem,
            FileSystemBasePath = basePath
        };
    }

    [Fact]
    public async Task S3Provider_ShouldReturnHealthyWithProviderS3()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: false);
        var sut = new InternalStorageHealthCheck(config);

        // Act
        var result = await sut.CheckHealthAsync(new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", sut, null, null)
        });

        // Assert
        result.Status.Should().Be(HealthStatus.Healthy);
        result.Description.Should().Contain("S3");
        result.Data.Should().ContainKey("Provider").WhoseValue.Should().Be("S3");
        result.Data.Should().ContainKey("Bucket");
    }

    [Fact]
    public async Task FileSystemProvider_ShouldReturnHealthyWithProviderFS()
    {
        // Arrange
        var basePath = Path.Combine(Path.GetTempPath(), $"healthcheck-test-{Guid.NewGuid():N}");
        var config = CreateConfig(useFileSystem: true, basePath: basePath);
        var sut = new InternalStorageHealthCheck(config);

        try
        {
            // Act
            var result = await sut.CheckHealthAsync(new HealthCheckContext
            {
                Registration = new HealthCheckRegistration("test", sut, null, null)
            });

            // Assert
            result.Status.Should().Be(HealthStatus.Healthy);
            result.Description.Should().Contain("FS");
            result.Data.Should().ContainKey("Provider").WhoseValue.Should().Be("FS");
            result.Data.Should().ContainKey("BasePath").WhoseValue.Should().Be(basePath);
        }
        finally
        {
            if (Directory.Exists(basePath))
                Directory.Delete(basePath, recursive: true);
        }
    }

    [Fact]
    public async Task FileSystemProvider_NullBasePath_ShouldUseDefaultTempPath()
    {
        // Arrange
        var config = CreateConfig(useFileSystem: true, basePath: null);
        var sut = new InternalStorageHealthCheck(config);

        // Act
        var result = await sut.CheckHealthAsync(new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", sut, null, null)
        });

        // Assert
        result.Status.Should().Be(HealthStatus.Healthy);
        result.Data.Should().ContainKey("Provider").WhoseValue.Should().Be("FS");
        result.Data["BasePath"].ToString().Should().Contain("keeper-data-bridge");
    }

    [Fact]
    public async Task FileSystemProvider_UnwritablePath_ShouldReturnUnhealthy()
    {
        // Arrange — use an invalid path that cannot be created
        var config = CreateConfig(useFileSystem: true, basePath: Path.Combine("Z:\\", "nonexistent", Guid.NewGuid().ToString()));
        var sut = new InternalStorageHealthCheck(config);

        // Act
        var result = await sut.CheckHealthAsync(new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", sut, null, null)
        });

        // Assert
        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("not writable");
        result.Data.Should().ContainKey("Error");
    }
}
