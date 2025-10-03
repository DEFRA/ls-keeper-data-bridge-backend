using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using System.Security.Claims;
using System.Text.Encodings.Web;

namespace KeeperData.Bridge.Authentication;

public class ApiKeyAuthenticationHandler : AuthenticationHandler<ApiKeyAuthenticationSchemeOptions>
{
    private const string AuthorizationHeaderName = "Authorization";
    private const string ApiKeyPrefix = "ApiKey ";

    public ApiKeyAuthenticationHandler(
        IOptionsMonitor<ApiKeyAuthenticationSchemeOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder)
        : base(options, logger, encoder)
    {
    }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!Request.Headers.TryGetValue(AuthorizationHeaderName, out var authHeaderValues))
        {
            return Task.FromResult(AuthenticateResult.Fail("Missing Authorization header"));
        }

        var authHeader = authHeaderValues.FirstOrDefault();
        if (string.IsNullOrWhiteSpace(authHeader))
        {
            return Task.FromResult(AuthenticateResult.Fail("Empty Authorization header"));
        }

        if (!authHeader.StartsWith(ApiKeyPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return Task.FromResult(AuthenticateResult.Fail("Invalid Authorization header format. Expected 'ApiKey <key>'"));
        }

        var providedApiKey = authHeader.Substring(ApiKeyPrefix.Length).Trim();
        if (string.IsNullOrWhiteSpace(providedApiKey))
        {
            return Task.FromResult(AuthenticateResult.Fail("Empty API key"));
        }

        var expectedApiKey = Context.RequestServices
            .GetRequiredService<IConfiguration>()["ApiAuthentication:ApiKey"];

        if (string.IsNullOrWhiteSpace(expectedApiKey))
        {
            Logger.LogError("API key not configured. Please set ApiAuthentication:ApiKey in configuration.");
            return Task.FromResult(AuthenticateResult.Fail("API key not configured"));
        }

        if (!string.Equals(providedApiKey, expectedApiKey, StringComparison.Ordinal))
        {
            Logger.LogWarning("Invalid API key provided");
            return Task.FromResult(AuthenticateResult.Fail("Invalid API key"));
        }

        var claims = new[]
        {
            new Claim(ClaimTypes.AuthenticationMethod, "ApiKey"),
            new Claim("ApiKeyId", "default")
        };

        var identity = new ClaimsIdentity(claims, Scheme.Name);
        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, Scheme.Name);

        Logger.LogDebug("API key authentication successful");
        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}