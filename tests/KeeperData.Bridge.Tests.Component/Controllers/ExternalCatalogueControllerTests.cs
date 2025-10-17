using FluentAssertions;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Configuration;
using System.Net;
using System.Net.Http.Headers;

namespace KeeperData.Bridge.Tests.Component.Controllers;

public class ExternalCatalogueControllerTests : IClassFixture<AppWebApplicationFactory>
{
    private readonly AppWebApplicationFactory _factory;
    private readonly HttpClient _client;
    private const string TestApiKey = "test-api-key-for-component-tests";

    public ExternalCatalogueControllerTests(AppWebApplicationFactory factory)
    {
        _factory = factory;

        var customFactory = factory.WithWebHostBuilder(builder =>
        {
            builder.ConfigureAppConfiguration((context, config) =>
            {
                // Add test API key configuration
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ApiAuthentication:ApiKey"] = TestApiKey
                });
            });
        });

        _client = customFactory.CreateClient();
    }

    [Fact]
    public async Task GetFilesReport_WithValidApiKeyAndValidInternalSourceType_ShouldReturnOk()
    {
        // Arrange
        _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("ApiKey", TestApiKey);

        // Act
        var response = await _client.GetAsync($"/api/externalcatalogue/files?sourceType={BlobStorageSources.Internal}");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var content = await response.Content.ReadAsStringAsync();
        content.Should().Contain("FILE REPORT FOR SOURCE: INTERNAL");
        content.Should().Contain("Generated on:");
        response.Content.Headers.ContentType?.MediaType.Should().Be("text/plain");
    }

    [Fact]
    public async Task GetFilesReport_WithValidApiKeyAndValidExternalSourceType_ShouldReturnOk()
    {
        // Arrange
        _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("ApiKey", TestApiKey);

        // Act
        var response = await _client.GetAsync($"/api/externalcatalogue/files?sourceType={BlobStorageSources.External}");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var content = await response.Content.ReadAsStringAsync();
        content.Should().Contain("FILE REPORT FOR SOURCE: EXTERNAL");
        content.Should().Contain("Generated on:");
        response.Content.Headers.ContentType?.MediaType.Should().Be("text/plain");
    }

    [Fact]
    public async Task GetFilesReport_WithValidApiKeyAndInvalidSourceType_ShouldReturnBadRequest()
    {
        // Arrange
        _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("ApiKey", TestApiKey);

        // Act
        var response = await _client.GetAsync("/api/externalcatalogue/files?sourceType=invalid");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var content = await response.Content.ReadAsStringAsync();
        content.Should().Contain("Invalid source type");
    }

    [Fact]
    public async Task GetFilesReport_WithValidApiKeyAndMissingSourceType_ShouldReturnBadRequest()
    {
        // Arrange
        _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("ApiKey", TestApiKey);

        // Act
        var response = await _client.GetAsync("/api/externalcatalogue/files");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var content = await response.Content.ReadAsStringAsync();
        // ASP.NET Core model validation returns structured JSON validation errors
        content.Should().Contain("sourceType");
        content.Should().Contain("required");
    }

    [Fact]
    public async Task GetFilesReport_WithValidApiKeyAndEmptySourceType_ShouldReturnBadRequest()
    {
        // Arrange
        _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("ApiKey", TestApiKey);

        // Act
        var response = await _client.GetAsync("/api/externalcatalogue/files?sourceType=");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var content = await response.Content.ReadAsStringAsync();
        // ASP.NET Core model validation returns structured JSON validation errors
        content.Should().Contain("sourceType");
        content.Should().Contain("required");
    }

}