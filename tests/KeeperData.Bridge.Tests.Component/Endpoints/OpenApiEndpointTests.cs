using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using System.Net;
using System.Text.Json;

namespace KeeperData.Bridge.Tests.Component.Endpoints;

public class OpenApiEndpointTests(AppWebApplicationFactory factory) : IClassFixture<AppWebApplicationFactory>
{
    private readonly AppWebApplicationFactory _factory = factory;

    private static async Task<JsonElement> GetDocumentAsync(HttpClient client)
    {
        var response = await client.GetAsync("openapi/v1.json");

        response.StatusCode.Should().Be(HttpStatusCode.OK, "the API contract is served without a key so consumers can read it");

        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());

        return document.RootElement.Clone();
    }

    private Task<JsonElement> GetDocumentAsync() => GetDocumentAsync(_factory.CreateClient());

    [Fact]
    public async Task GivenOpenApiRequest_ShouldDescribeTheApi()
    {
        var root = await GetDocumentAsync();

        root.GetProperty("openapi").GetString().Should().StartWith("3.1");
        root.GetProperty("info").GetProperty("title").GetString().Should().Be("KeeperData Bridge API");
        root.GetProperty("info").GetProperty("contact").GetProperty("name").GetString().Should().Be("DEFRA");
    }

    [Fact]
    public async Task GivenOpenApiRequest_ShouldIncludeControllerRoutes()
    {
        var paths = (await GetDocumentAsync()).GetProperty("paths");

        paths.TryGetProperty("/api/etl/imports", out _).Should().BeTrue();
        paths.TryGetProperty("/api/Import", out _).Should().BeTrue();
        paths.TryGetProperty("/api/Benchmark/report", out _).Should().BeTrue("Mongo BsonDocument responses must still be describable");
    }

    [Fact]
    public async Task GivenOpenApiRequest_ShouldRequireThePurgeScope()
    {
        var operation = (await GetDocumentAsync())
            .GetProperty("paths")
            .GetProperty("/api/etl/storage")
            .GetProperty("delete");

        var parameters = operation.GetProperty("parameters").EnumerateArray().ToArray();

        parameters.Single(parameter => parameter.GetProperty("name").GetString() == "dataset")
            .GetProperty("required").GetBoolean().Should().BeTrue();
        parameters.Single(parameter => parameter.GetProperty("name").GetString() == "stage")
            .GetProperty("required").GetBoolean().Should().BeTrue();
    }

    [Fact]
    public async Task GivenOpenApiRequest_ShouldNotDescribeTheDocumentEndpointItself()
    {
        var paths = (await GetDocumentAsync()).GetProperty("paths");

        paths.TryGetProperty("/openapi/{documentName}.json", out _).Should().BeFalse();
    }

    [Fact]
    public async Task GivenAuthenticationEnabled_ShouldDocumentTheApiKeyRequirement()
    {
        var client = _factory.WithWebHostBuilder(builder =>
            builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(
                new Dictionary<string, string?> { ["FeatureFlags:AuthenticationEnabled"] = "true" })))
            .CreateClient();

        var root = await GetDocumentAsync(client);

        var scheme = root.GetProperty("components").GetProperty("securitySchemes").GetProperty("ApiKey");
        scheme.GetProperty("type").GetString().Should().Be("http");
        scheme.GetProperty("scheme").GetString().Should()
            .Be("ApiKey", "ApiKeyAuthenticationHandler requires 'Authorization: ApiKey <key>'");

        root.GetProperty("security").EnumerateArray()
            .Should().ContainSingle().Which.TryGetProperty("ApiKey", out _).Should().BeTrue();
    }

    [Fact]
    public async Task GivenAuthenticationDisabled_ShouldNotDocumentAnApiKeyRequirement()
    {
        var root = await GetDocumentAsync();

        root.TryGetProperty("security", out _).Should().BeFalse();
    }

    [Fact]
    public async Task GivenSwaggerUiRequest_ShouldNoLongerBeServed()
    {
        var response = await _factory.CreateClient().GetAsync("swagger");

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }
}
