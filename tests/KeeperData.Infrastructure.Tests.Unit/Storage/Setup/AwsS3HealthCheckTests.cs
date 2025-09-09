using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Setup;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;

namespace KeeperData.Infrastructure.Tests.Unit.Storage.Setup;

public class AwsS3HealthCheckTests
{
    private readonly Mock<IAmazonS3> _amazonS3Mock;
    private readonly Mock<IS3ClientFactory> _s3ClientFactoryMock;

    private readonly HealthCheckContext _healthCheckContext = new();

    private readonly AwsS3HealthCheck _sut;

    private const string ExternalStorageBucket = "test-external-bucket";

    public AwsS3HealthCheckTests()
    {
        _amazonS3Mock = new Mock<IAmazonS3>();

        _amazonS3Mock
            .Setup(x => x.GetBucketAclAsync(It.IsAny<GetBucketAclRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetBucketAclResponse { HttpStatusCode = HttpStatusCode.OK });

        _amazonS3Mock
            .Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.OK });

        _s3ClientFactoryMock = new Mock<IS3ClientFactory>();

        _s3ClientFactoryMock.Setup(x => x.GetRegisteredClientNames())
            .Returns([typeof(ExternalStorageClient).Name]);

        _s3ClientFactoryMock.Setup(x => x.GetClient(It.IsAny<string>()))
            .Returns(_amazonS3Mock.Object);

        _s3ClientFactoryMock.Setup(x => x.GetClientBucketName(It.IsAny<string>()))
            .Returns(ExternalStorageBucket);

        _sut = new AwsS3HealthCheck(_s3ClientFactoryMock.Object, new Mock<ILogger<AwsS3HealthCheck>>().Object);
    }

    [Fact]
    public async Task GivenValidBucketName_WhenCallingCheckHealthAsync_ShouldSucceed()
    {
        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task GivenS3BucketsAreNotFound_WhenRequestingHealthCheck_ShouldFail()
    {
        _amazonS3Mock
            .Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.NotFound });

        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Be($"Some S3 buckets failed: {ExternalStorageBucket}");
    }

    [Fact]
    public async Task GivenS3ClientThrowsAmazonS3Exception_WhenRequestingHealthCheck_ShouldFail()
    {
        _amazonS3Mock
            .Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.NotFound });

        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Be($"Some S3 buckets failed: {ExternalStorageBucket}");
    }

    [Fact]
    public async Task GivenS3ClientThrowsException_WhenRequestingHealthCheck_ShouldFail()
    {
        _amazonS3Mock
            .Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Exception message"));

        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Be($"Some S3 buckets failed: {ExternalStorageBucket}");
    }
}
