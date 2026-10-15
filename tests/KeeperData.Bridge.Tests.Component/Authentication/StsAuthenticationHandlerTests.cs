using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using FluentAssertions;
using KeeperData.Bridge.Authentication;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace KeeperData.Bridge.Tests.Component.Authentication;

public class StsAuthenticationHandlerTests
{
    private const string SamplePresignedUrl =
        "https://sts.eu-west-2.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc";

    private readonly Mock<IStsIdentityVerifier> _verifierMock = new();
    private readonly IServiceCollection _services = new ServiceCollection();

    public StsAuthenticationHandlerTests()
    {
        _services.AddLogging(builder => builder.AddConsole());
        _services.AddSingleton(_verifierMock.Object);
        _services.AddAuthentication()
            .AddScheme<StsAuthenticationSchemeOptions, StsAuthenticationHandler>(
                StsAuthenticationSchemeOptions.DefaultScheme,
                _ => { });
    }

    private async Task<AuthenticateResult> CreateAndAuthenticateAsync(string? authorizationHeader = null)
    {
        var serviceProvider = _services.BuildServiceProvider();
        var httpContext = new DefaultHttpContext
        {
            RequestServices = serviceProvider
        };

        if (authorizationHeader != null)
        {
            httpContext.Request.Headers["Authorization"] = authorizationHeader;
        }

        var options = serviceProvider.GetRequiredService<IOptionsMonitor<StsAuthenticationSchemeOptions>>();
        var logger = serviceProvider.GetRequiredService<ILoggerFactory>();
        var encoder = serviceProvider.GetRequiredService<UrlEncoder>();

        var handler = new StsAuthenticationHandler(options, logger, encoder, _verifierMock.Object);
        var scheme = new AuthenticationScheme(
            StsAuthenticationSchemeOptions.DefaultScheme,
            StsAuthenticationSchemeOptions.DefaultScheme,
            typeof(StsAuthenticationHandler));

        await handler.InitializeAsync(scheme, httpContext);
        return await handler.AuthenticateAsync();
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithMissingHeader_ShouldReturnNoResult()
    {
        var result = await CreateAndAuthenticateAsync();

        result.None.Should().BeTrue();
        _verifierMock.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithEmptyHeader_ShouldReturnNoResult()
    {
        var result = await CreateAndAuthenticateAsync("");

        result.None.Should().BeTrue();
        _verifierMock.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithApiKeyPrefix_ShouldReturnNoResultSoApiKeySchemeHandlesIt()
    {
        var result = await CreateAndAuthenticateAsync("ApiKey some-api-key");

        result.None.Should().BeTrue();
        _verifierMock.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithEmptyStsToken_ShouldFail()
    {
        var result = await CreateAndAuthenticateAsync("AwsSts   ");

        result.Succeeded.Should().BeFalse();
        result.Failure!.Message.Should().Be("Empty STS token");
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WithInvalidBase64Token_ShouldFail()
    {
        var result = await CreateAndAuthenticateAsync("AwsSts !!!not-base64!!!");

        result.Succeeded.Should().BeFalse();
        result.Failure!.Message.Should().Be("Invalid STS token encoding");
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WhenVerifierFails_ShouldFailWithReason()
    {
        var token = WebEncoders.Base64UrlEncode(Encoding.UTF8.GetBytes(SamplePresignedUrl));
        _verifierMock
            .Setup(v => v.VerifyAsync(SamplePresignedUrl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(StsVerificationResult.Fail("Caller identity is not on the allowlist"));

        var result = await CreateAndAuthenticateAsync($"AwsSts {token}");

        result.Succeeded.Should().BeFalse();
        result.Failure!.Message.Should().Be("Caller identity is not on the allowlist");
    }

    [Fact]
    public async Task HandleAuthenticateAsync_WhenVerifierSucceeds_ShouldPopulateClaims()
    {
        var token = WebEncoders.Base64UrlEncode(Encoding.UTF8.GetBytes(SamplePresignedUrl));
        var identity = new StsCallerIdentity(
            Arn: "arn:aws:sts::332499610595:assumed-role/ls-keeper-data-api/session-1",
            Account: "332499610595",
            UserId: "AROAEXAMPLE:session-1",
            RoleName: "ls-keeper-data-api");

        _verifierMock
            .Setup(v => v.VerifyAsync(SamplePresignedUrl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(StsVerificationResult.Success(identity));

        var result = await CreateAndAuthenticateAsync($"awssts {token}");

        result.Succeeded.Should().BeTrue();
        result.Principal.Should().NotBeNull();
        result.Principal!.Identity!.IsAuthenticated.Should().BeTrue();
        result.Principal.Identity.AuthenticationType.Should().Be(StsAuthenticationSchemeOptions.DefaultScheme);
        result.Principal.Identity.Name.Should().Be("ls-keeper-data-api");
        result.Principal.FindFirstValue(ClaimTypes.AuthenticationMethod).Should().Be("AwsSts");
        result.Principal.FindFirstValue("aws:arn").Should().Be(identity.Arn);
        result.Principal.FindFirstValue("aws:account").Should().Be("332499610595");
        result.Principal.FindFirstValue("aws:userid").Should().Be("AROAEXAMPLE:session-1");
    }
}