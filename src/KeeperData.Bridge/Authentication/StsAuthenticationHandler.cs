using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Options;

namespace KeeperData.Bridge.Authentication;

public class StsAuthenticationHandler : AuthenticationHandler<StsAuthenticationSchemeOptions>
{
    private const string AuthorizationHeaderName = "Authorization";
    private const string StsPrefix = StsAuthenticationSchemeOptions.DefaultScheme + " ";

    private readonly IStsIdentityVerifier _verifier;

    public StsAuthenticationHandler(
        IOptionsMonitor<StsAuthenticationSchemeOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder,
        IStsIdentityVerifier verifier)
        : base(options, logger, encoder)
    {
        _verifier = verifier;
    }

    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!Request.Headers.TryGetValue(AuthorizationHeaderName, out var authHeaderValues))
        {
            return AuthenticateResult.NoResult();
        }

        var authHeader = authHeaderValues.FirstOrDefault();
        if (string.IsNullOrWhiteSpace(authHeader))
        {
            return AuthenticateResult.NoResult();
        }

        if (!authHeader.StartsWith(StsPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return AuthenticateResult.NoResult();
        }

        var encodedToken = authHeader[StsPrefix.Length..].Trim();
        if (string.IsNullOrWhiteSpace(encodedToken))
        {
            return AuthenticateResult.Fail("Empty STS token");
        }

        if (!TryDecodePresignedUrl(encodedToken, out var presignedUrl))
        {
            Logger.LogWarning("Invalid base64url encoding in AwsSts Authorization header");
            return AuthenticateResult.Fail("Invalid STS token encoding");
        }

        var verification = await _verifier.VerifyAsync(presignedUrl, Context.RequestAborted);
        if (!verification.Succeeded || verification.Identity is null)
        {
            return AuthenticateResult.Fail(verification.FailureReason ?? "STS authentication failed");
        }

        var identity = verification.Identity;
        var claims = new[]
        {
            new Claim(ClaimTypes.AuthenticationMethod, StsAuthenticationSchemeOptions.DefaultScheme),
            new Claim(ClaimTypes.Name, identity.RoleName),
            new Claim("aws:arn", identity.Arn),
            new Claim("aws:account", identity.Account),
            new Claim("aws:userid", identity.UserId)
        };

        var claimsIdentity = new ClaimsIdentity(claims, Scheme.Name);
        var principal = new ClaimsPrincipal(claimsIdentity);
        var ticket = new AuthenticationTicket(principal, Scheme.Name);

        Logger.LogDebug("STS authentication successful for {Arn}", identity.Arn);
        return AuthenticateResult.Success(ticket);
    }

    private static bool TryDecodePresignedUrl(string encodedToken, out string presignedUrl)
    {
        try
        {
            var normalized = encodedToken.Replace('+', '-').Replace('/', '_').TrimEnd('=');
            var bytes = WebEncoders.Base64UrlDecode(normalized);
            presignedUrl = Encoding.UTF8.GetString(bytes);
            return !string.IsNullOrWhiteSpace(presignedUrl);
        }
        catch (FormatException)
        {
            presignedUrl = string.Empty;
            return false;
        }
    }
}