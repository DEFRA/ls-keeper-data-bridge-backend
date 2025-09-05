using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using Moq;
using System.Net;

namespace KeeperData.Bridge.Tests.Component.Endpoints;

public class HealthcheckEndpointTests(AppTestFixture appTestFixture) : IClassFixture<AppTestFixture>
{
    private readonly AppTestFixture _appTestFixture = appTestFixture;

    [Fact]
    public async Task GivenValidHealthCheckRequest_ShouldSucceed()
    {
        _appTestFixture.AppWebApplicationFactory.AmazonS3Mock!
            .Setup(x => x.GetBucketAclAsync(It.IsAny<GetBucketAclRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetBucketAclResponse { HttpStatusCode = HttpStatusCode.OK });

        _appTestFixture.AppWebApplicationFactory.AmazonS3Mock!
            .Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.OK });

        var response = await _appTestFixture.HttpClient.GetAsync("health");
        var responseBody = await response.Content.ReadAsStringAsync();

        response.EnsureSuccessStatusCode();
        responseBody.Should().NotBeNullOrEmpty().And.Contain("\"status\": \"Healthy\"");
        responseBody.Should().Contain("All S3 buckets are reachable");
    }

    [Fact]
    public async Task GivenS3BucketsAreNotFound_WhenRequestingHealthCheck_ShouldFail()
    {
        _appTestFixture.AppWebApplicationFactory.AmazonS3Mock!
            .Setup(x => x.GetBucketAclAsync(It.IsAny<GetBucketAclRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetBucketAclResponse { HttpStatusCode = HttpStatusCode.NotFound });

        _appTestFixture.AppWebApplicationFactory.AmazonS3Mock!
            .Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.NotFound });

        var response = await _appTestFixture.HttpClient.GetAsync("health");
        var responseBody = await response.Content.ReadAsStringAsync();

        response.StatusCode.Should().Be(System.Net.HttpStatusCode.ServiceUnavailable);
        responseBody.Should().NotBeNullOrEmpty().And.Contain("\"status\": \"Unhealthy\"");
        responseBody.Should().Contain("Some S3 buckets failed");
    }

    [Fact]
    public async Task GivenS3ClientThrowsAmazonS3Exception_WhenRequestingHealthCheck_ShouldFail()
    {
        _appTestFixture.AppWebApplicationFactory.AmazonS3Mock!
            .Setup(x => x.GetBucketAclAsync(It.IsAny<GetBucketAclRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonS3Exception("Exception message") { StatusCode = HttpStatusCode.NotFound });

        _appTestFixture.AppWebApplicationFactory.AmazonS3Mock!
            .Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.NotFound });

        var response = await _appTestFixture.HttpClient.GetAsync("health");
        var responseBody = await response.Content.ReadAsStringAsync();

        response.StatusCode.Should().Be(System.Net.HttpStatusCode.ServiceUnavailable);
        responseBody.Should().NotBeNullOrEmpty().And.Contain("\"status\": \"Unhealthy\"");
        responseBody.Should().Contain("Some S3 buckets failed");
    }

    [Fact]
    public async Task GivenS3ClientThrowsException_WhenRequestingHealthCheck_ShouldFail()
    {
        _appTestFixture.AppWebApplicationFactory.AmazonS3Mock!
            .Setup(x => x.GetBucketAclAsync(It.IsAny<GetBucketAclRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Exception message"));

        _appTestFixture.AppWebApplicationFactory.AmazonS3Mock!
            .Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Exception message"));

        var response = await _appTestFixture.HttpClient.GetAsync("health");
        var responseBody = await response.Content.ReadAsStringAsync();

        response.StatusCode.Should().Be(System.Net.HttpStatusCode.ServiceUnavailable);
        responseBody.Should().NotBeNullOrEmpty().And.Contain("\"status\": \"Unhealthy\"");
        responseBody.Should().Contain("Some S3 buckets failed");
    }
}