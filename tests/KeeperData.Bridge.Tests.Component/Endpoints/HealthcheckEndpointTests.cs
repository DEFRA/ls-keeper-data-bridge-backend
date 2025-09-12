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
            .Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.OK });

        var response = await _appTestFixture.HttpClient.GetAsync("health");
        var responseBody = await response.Content.ReadAsStringAsync();

        response.EnsureSuccessStatusCode();
        responseBody.Should().NotBeNullOrEmpty().And.Contain("\"status\": \"Healthy\"");
        responseBody.Should().Contain("All S3 buckets are reachable");
        responseBody.Should().Contain("SNS topic \\u0027ls-keeper-data-bridge-events\\u0027 is reachable.");
        responseBody.Should().Contain("\"ExternalStorageClient\":");
        responseBody.Should().Contain("\"InternalStorageClient\":");
        responseBody.Should().Contain("MongoDB is reachable");
    }
}