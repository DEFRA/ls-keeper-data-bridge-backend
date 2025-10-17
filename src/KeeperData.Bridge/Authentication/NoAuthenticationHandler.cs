using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using System.Security.Claims;
using System.Text.Encodings.Web;

namespace KeeperData.Bridge.Authentication;

/// <summary>
/// Authentication handler that allows all requests through without requiring authentication.
/// Used when AuthenticationEnabled feature flag is set to false.
/// </summary>
public class NoAuthenticationHandler : AuthenticationHandler<AuthenticationSchemeOptions>
{
    public NoAuthenticationHandler(
        IOptionsMonitor<AuthenticationSchemeOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder)
        : base(options, logger, encoder)
    {
    }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        // Create a simple anonymous identity
        var claims = new[]
        {
            new Claim(ClaimTypes.Name, "Anonymous"),
            new Claim("AuthenticationDisabled", "true")
        };

        var identity = new ClaimsIdentity(claims, Scheme.Name);
        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, Scheme.Name);

        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}
