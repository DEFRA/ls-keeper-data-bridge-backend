using FluentAssertions;

namespace KeeperData.Bridge.Tests.Integration.Endpoints;

[Trait("Dependence", "localstack")]
public class HealthcheckEndpointTests(IntegrationTestFixture fixture) : IClassFixture<IntegrationTestFixture>
{
    private readonly HttpClient _httpClient = fixture.HttpClient;

    [Fact(Skip = "Does not work locally")]
    public async Task GivenValidHealthCheckRequest_ShouldSucceed()
    {
        var response = await _httpClient.GetAsync("health");
        var responseBody = await response.Content.ReadAsStringAsync();

        response.EnsureSuccessStatusCode();
        responseBody.Should().NotBeNullOrEmpty().And.Contain("\"status\": \"Healthy\"");
    }
}