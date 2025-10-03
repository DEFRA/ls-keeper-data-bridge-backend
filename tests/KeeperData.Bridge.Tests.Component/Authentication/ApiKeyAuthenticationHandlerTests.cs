using FluentAssertions;
using KeeperData.Bridge.Authentication;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Encodings.Web;

namespace KeeperData.Bridge.Tests.Component.Authentication;

public class ApiKeyAuthenticationHandlerTests
{
    private readonly IServiceCollection _services;
    private readonly IConfiguration _configuration;

    private const string TestApiKey = "test-api-key-12345";

    public ApiKeyAuthenticationHandlerTests()
    {
        _services = new ServiceCollection();

        var configData = new Dictionary<string, string?>
        {
            ["ApiAuthentication:ApiKey"] = TestApiKey
        };

        _configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        _services.AddSingleton(_configuration);
        _services.AddLogging(builder => builder.AddConsole());
        _services.AddAuthentication()
            .AddScheme<ApiKeyAuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(
                ApiKeyAuthenticationSchemeOptions.DefaultScheme,
                options => { });
    }

    private async Task<AuthenticateResult> CreateAndAuthenticateAsync(string? authorizationHeader = null)
    {
        var serviceProvider = _services.BuildServiceProvider();
        var httpContext = new DefaultHttpContext();
        httpContext.RequestServices = serviceProvider;

        if (authorizationHeader != null)
        {
            httpContext.Request.Headers["Authorization"] = authorizationHeader;
        }

        var options = serviceProvider.GetRequiredService<IOptionsMonitor<ApiKeyAuthenticationSchemeOptions>>();
        var logger = serviceProvider.GetRequiredService<ILoggerFactory>();
        var encoder = serviceProvider.GetRequiredService<UrlEncoder>();

        var handler = new ApiKeyAuthenticationHandler(options, logger, encoder);

        // Initialize the handler with a fake scheme
        var scheme = new AuthenticationScheme(
            ApiKeyAuthenticationSchemeOptions.DefaultScheme,
            ApiKeyAuthenticationSchemeOptions.DefaultScheme,
            typeof(ApiKeyAuthenticationHandler));

        await handler.InitializeAsync(scheme, httpContext);

        return await handler.AuthenticateAsync();
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithValidApiKey_ShouldSucceed()
    {
        // Act
        var result = await CreateAndAuthenticateAsync($"ApiKey {TestApiKey}");

        // Assert
        result.Succeeded.Should().BeTrue();
        result.Principal.Should().NotBeNull();
        result.Principal!.Identity!.IsAuthenticated.Should().BeTrue();
        result.Principal.Identity.AuthenticationType.Should().Be(ApiKeyAuthenticationSchemeOptions.DefaultScheme);
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithInvalidApiKey_ShouldFail()
    {
        // Act
        var result = await CreateAndAuthenticateAsync("ApiKey invalid-key");

        // Assert
        result.Succeeded.Should().BeFalse();
        result.Failure.Should().NotBeNull();
        result.Failure!.Message.Should().Be("Invalid API key");
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithMissingAuthorizationHeader_ShouldFail()
    {
        // Act
        var result = await CreateAndAuthenticateAsync();

        // Assert
        result.Succeeded.Should().BeFalse();
        result.Failure.Should().NotBeNull();
        result.Failure!.Message.Should().Be("Missing Authorization header");
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithEmptyAuthorizationHeader_ShouldFail()
    {
        // Act
        var result = await CreateAndAuthenticateAsync("");

        // Assert
        result.Succeeded.Should().BeFalse();
        result.Failure.Should().NotBeNull();
        result.Failure!.Message.Should().Be("Empty Authorization header");
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithWrongAuthorizationFormat_ShouldFail()
    {
        // Act
        var result = await CreateAndAuthenticateAsync($"Bearer {TestApiKey}");

        // Assert
        result.Succeeded.Should().BeFalse();
        result.Failure.Should().NotBeNull();
        result.Failure!.Message.Should().Be("Invalid Authorization header format. Expected 'ApiKey <key>'");
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithEmptyApiKeyInHeader_ShouldFail()
    {
        // Act
        var result = await CreateAndAuthenticateAsync("ApiKey ");

        // Assert
        result.Succeeded.Should().BeFalse();
        result.Failure.Should().NotBeNull();
        result.Failure!.Message.Should().Be("Empty API key");
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithMissingApiKeyConfiguration_ShouldFail()
    {
        // Arrange - Create handler with empty configuration
        var services = new ServiceCollection();
        var emptyConfig = new ConfigurationBuilder().Build();
        services.AddSingleton<IConfiguration>(emptyConfig);
        services.AddLogging(builder => builder.AddConsole());
        services.AddAuthentication()
            .AddScheme<ApiKeyAuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(
                ApiKeyAuthenticationSchemeOptions.DefaultScheme,
                options => { });

        var serviceProvider = services.BuildServiceProvider();
        var httpContext = new DefaultHttpContext();
        httpContext.RequestServices = serviceProvider;
        httpContext.Request.Headers["Authorization"] = $"ApiKey {TestApiKey}";

        var options = serviceProvider.GetRequiredService<IOptionsMonitor<ApiKeyAuthenticationSchemeOptions>>();
        var logger = serviceProvider.GetRequiredService<ILoggerFactory>();
        var encoder = serviceProvider.GetRequiredService<UrlEncoder>();

        var handler = new ApiKeyAuthenticationHandler(options, logger, encoder);
        var scheme = new AuthenticationScheme(
            ApiKeyAuthenticationSchemeOptions.DefaultScheme,
            ApiKeyAuthenticationSchemeOptions.DefaultScheme,
            typeof(ApiKeyAuthenticationHandler));

        await handler.InitializeAsync(scheme, httpContext);

        // Act
        var result = await handler.AuthenticateAsync();

        // Assert
        result.Succeeded.Should().BeFalse();
        result.Failure.Should().NotBeNull();
        result.Failure!.Message.Should().Be("API key not configured");
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithCaseInsensitiveApiKeyPrefix_ShouldWork()
    {
        // Act
        var result = await CreateAndAuthenticateAsync($"apikey {TestApiKey}");

        // Assert
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithExtraWhitespaceInApiKey_ShouldWork()
    {
        // Act
        var result = await CreateAndAuthenticateAsync($"ApiKey   {TestApiKey}   ");

        // Assert
        result.Succeeded.Should().BeTrue();
    }
}