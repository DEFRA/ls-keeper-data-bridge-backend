using FluentAssertions;

namespace KeeperData.Bridge.Tests.Component.Endpoints;

public class HealthcheckEndpointTests(AppTestFixture appTestFixture) : IClassFixture<AppTestFixture>
{
    private readonly AppTestFixture _appTestFixture = appTestFixture;

    [Fact]
    public async Task GivenValidHealthCheckRequest_ShouldSucceed()
    {
        var response = await _appTestFixture.HttpClient.GetAsync("health");
        var responseBody = await response.Content.ReadAsStringAsync();

        response.EnsureSuccessStatusCode();
        responseBody.Should().NotBeNullOrEmpty().And.Contain("\"status\": \"Healthy\"");
        responseBody.Should().Contain("All S3 buckets are reachable");
    }
}