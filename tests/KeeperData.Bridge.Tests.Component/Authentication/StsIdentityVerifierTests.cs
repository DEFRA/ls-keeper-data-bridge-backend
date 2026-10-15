using System.Net;
using System.Text;
using FluentAssertions;
using KeeperData.Bridge.Authentication;
using KeeperData.Bridge.Config;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace KeeperData.Bridge.Tests.Component.Authentication;

public class StsIdentityVerifierTests
{
    private static readonly DateTimeOffset FixedNow = new(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);
    private const string SignedDate = "20261014T120000Z";
    private const string DefaultAccount = "332499610595";
    private const string DefaultRole = "ls-keeper-data-api";
    private const string DefaultArn = $"arn:aws:sts::{DefaultAccount}:assumed-role/{DefaultRole}/task-123";

    private static string BuildPresignedUrl(
        string endpoint = "https://sts.eu-west-2.amazonaws.com/",
        string action = "GetCallerIdentity",
        string algorithm = "AWS4-HMAC-SHA256",
        string credential = "AKIAIOSFODNN7EXAMPLE/20261014/eu-west-2/sts/aws4_request",
        string amzDate = SignedDate,
        string expires = "60",
        string signedHeaders = "host",
        string signature = "0123456789abcdef")
    {
        return $"{endpoint}?Action={action}&Version=2011-06-15"
            + $"&X-Amz-Algorithm={algorithm}"
            + $"&X-Amz-Credential={Uri.EscapeDataString(credential)}"
            + $"&X-Amz-Date={amzDate}"
            + $"&X-Amz-Expires={expires}"
            + $"&X-Amz-SignedHeaders={Uri.EscapeDataString(signedHeaders)}"
            + $"&X-Amz-Signature={signature}";
    }

    private static string BuildStsXml(string arn = DefaultArn, string account = DefaultAccount, string userId = "AROAEXAMPLE:task-123") =>
        $"""
        <GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
          <GetCallerIdentityResult>
            <Arn>{arn}</Arn>
            <UserId>{userId}</UserId>
            <Account>{account}</Account>
          </GetCallerIdentityResult>
        </GetCallerIdentityResponse>
        """;

    private static (StsIdentityVerifier Verifier, RecordingHttpMessageHandler Handler, FakeTimeProvider Clock) CreateVerifier(
        Action<StsAuthenticationOptions>? configure = null,
        Func<HttpRequestMessage, HttpResponseMessage>? responder = null)
    {
        var options = new ApiAuthenticationOptions();
        options.Sts.AllowedCallers.Add(new AllowedStsCaller
        {
            AccountId = DefaultAccount,
            RolePattern = "ls-keeper-*"
        });
        configure?.Invoke(options.Sts);

        var monitor = new Mock<IOptionsMonitor<ApiAuthenticationOptions>>();
        monitor.Setup(m => m.CurrentValue).Returns(options);

        var handler = new RecordingHttpMessageHandler(responder ?? (_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(BuildStsXml(), Encoding.UTF8, "text/xml")
        }));

        var factory = new Mock<IHttpClientFactory>();
        factory.Setup(f => f.CreateClient(StsIdentityVerifier.HttpClientName))
            .Returns(() => new HttpClient(handler, disposeHandler: false));

        var clock = new FakeTimeProvider(FixedNow);
        var verifier = new StsIdentityVerifier(factory.Object, monitor.Object, NullLogger<StsIdentityVerifier>.Instance, clock);
        return (verifier, handler, clock);
    }

    [Fact]
    public async Task VerifyAsync_WithValidPresignedUrlAndAllowlistedCaller_ShouldSucceed()
    {
        var (verifier, handler, _) = CreateVerifier();

        var result = await verifier.VerifyAsync(BuildPresignedUrl());

        result.Succeeded.Should().BeTrue();
        result.Identity.Should().NotBeNull();
        result.Identity!.Account.Should().Be(DefaultAccount);
        result.Identity.RoleName.Should().Be(DefaultRole);
        result.Identity.Arn.Should().Be(DefaultArn);
        handler.CallCount.Should().Be(1);
    }

    [Fact]
    public async Task VerifyAsync_RepeatedWithSameUrlWithinTtl_ShouldUseCacheAndCallStsOnce()
    {
        var (verifier, handler, _) = CreateVerifier();
        var url = BuildPresignedUrl();

        var first = await verifier.VerifyAsync(url);
        var second = await verifier.VerifyAsync(url);

        first.Succeeded.Should().BeTrue();
        second.Succeeded.Should().BeTrue();
        handler.CallCount.Should().Be(1);
    }

    [Theory]
    [InlineData("not-a-url", "Malformed presigned URL")]
    [InlineData("http://sts.eu-west-2.amazonaws.com/?Action=GetCallerIdentity", "Presigned URL does not match configured STS endpoint")]
    [InlineData("https://evil.example.com/?Action=GetCallerIdentity", "Presigned URL does not match configured STS endpoint")]
    [InlineData("https://sts.eu-west-2.amazonaws.com:8443/?Action=GetCallerIdentity", "Presigned URL does not match configured STS endpoint")]
    [InlineData("https://sts.eu-west-2.amazonaws.com/subpath?Action=GetCallerIdentity", "Presigned URL must target the root path")]
    public async Task VerifyAsync_WithInvalidHostSchemeOrPath_ShouldRejectBeforeCallingNetwork(string url, string expectedMessagePart)
    {
        var (verifier, handler, _) = CreateVerifier();

        var result = await verifier.VerifyAsync(url);

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Contain(expectedMessagePart);
        handler.CallCount.Should().Be(0, "SSRF guard must reject before any outbound HTTP call");
    }

    [Fact]
    public async Task VerifyAsync_WithWrongAction_ShouldRejectBeforeCallingNetwork()
    {
        var (verifier, handler, _) = CreateVerifier();

        var result = await verifier.VerifyAsync(BuildPresignedUrl(action: "AssumeRole"));

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Contain("Action=GetCallerIdentity");
        handler.CallCount.Should().Be(0);
    }

    [Fact]
    public async Task VerifyAsync_WithNonStsCredentialScope_ShouldRejectBeforeCallingNetwork()
    {
        var (verifier, handler, _) = CreateVerifier();

        var result = await verifier.VerifyAsync(
            BuildPresignedUrl(credential: "AKIAEXAMPLE/20261014/eu-west-2/s3/aws4_request"));

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Contain("X-Amz-Credential");
        handler.CallCount.Should().Be(0);
    }

    [Fact]
    public async Task VerifyAsync_WithExcessiveExpires_ShouldRejectBeforeCallingNetwork()
    {
        var (verifier, handler, _) = CreateVerifier();

        var result = await verifier.VerifyAsync(BuildPresignedUrl(expires: "3600"));

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Contain("X-Amz-Expires");
        handler.CallCount.Should().Be(0);
    }

    [Fact]
    public async Task VerifyAsync_WithExpiredTimestamp_ShouldRejectBeforeCallingNetwork()
    {
        var (verifier, handler, _) = CreateVerifier();

        var result = await verifier.VerifyAsync(BuildPresignedUrl(amzDate: "20261014T110000Z", expires: "60"));

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Be("Presigned URL has expired");
        handler.CallCount.Should().Be(0);
    }

    [Fact]
    public async Task VerifyAsync_WithFutureTimestampBeyondClockSkew_ShouldRejectBeforeCallingNetwork()
    {
        var (verifier, handler, _) = CreateVerifier();

        var result = await verifier.VerifyAsync(BuildPresignedUrl(amzDate: "20261014T121000Z", expires: "60"));

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Be("Presigned URL X-Amz-Date is in the future");
        handler.CallCount.Should().Be(0);
    }

    [Fact]
    public async Task VerifyAsync_WithoutSignedHostHeader_ShouldRejectBeforeCallingNetwork()
    {
        var (verifier, handler, _) = CreateVerifier();

        var result = await verifier.VerifyAsync(BuildPresignedUrl(signedHeaders: "x-amz-date"));

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Contain("host header");
        handler.CallCount.Should().Be(0);
    }

    [Fact]
    public async Task VerifyAsync_WithEmptyAllowlist_ShouldFailClosedBeforeCallingNetwork()
    {
        var (verifier, handler, _) = CreateVerifier(sts => sts.AllowedCallers.Clear());

        var result = await verifier.VerifyAsync(BuildPresignedUrl());

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Be("No allowed STS callers configured");
        handler.CallCount.Should().Be(0);
    }

    [Fact]
    public async Task VerifyAsync_WhenCallerAccountNotAllowlisted_ShouldFail()
    {
        var (verifier, _, _) = CreateVerifier(
            responder: _ => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    BuildStsXml(arn: "arn:aws:sts::999999999999:assumed-role/ls-keeper-data-api/s", account: "999999999999"),
                    Encoding.UTF8,
                    "text/xml")
            });

        var result = await verifier.VerifyAsync(BuildPresignedUrl());

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Be("Caller identity is not on the allowlist");
    }

    [Fact]
    public async Task VerifyAsync_WhenCallerRoleNotAllowlisted_ShouldFail()
    {
        var (verifier, _, _) = CreateVerifier(
            responder: _ => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    BuildStsXml(arn: $"arn:aws:sts::{DefaultAccount}:assumed-role/unrelated-service/s"),
                    Encoding.UTF8,
                    "text/xml")
            });

        var result = await verifier.VerifyAsync(BuildPresignedUrl());

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Be("Caller identity is not on the allowlist");
    }

    [Fact]
    public async Task VerifyAsync_WhenStsReturnsForbidden_ShouldFail()
    {
        var (verifier, _, _) = CreateVerifier(
            responder: _ => new HttpResponseMessage(HttpStatusCode.Forbidden));

        var result = await verifier.VerifyAsync(BuildPresignedUrl());

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Be("STS rejected request with status 403");
    }

    [Theory]
    [InlineData("arn:aws:sts::332499610595:assumed-role/ls-keeper-data-api/task-1", "ls-keeper-data-api")]
    [InlineData("arn:aws:iam::332499610595:role/service-role/my-role", "my-role")]
    [InlineData("arn:aws:iam::332499610595:user/dev-user", "dev-user")]
    [InlineData("arn:aws:iam::000000000000:root", "root")]
    public void ExtractRoleName_ShouldParseStandardIamAndStsArns(string arn, string expectedRole)
    {
        StsIdentityVerifier.ExtractRoleName(arn).Should().Be(expectedRole);
    }

    private sealed class RecordingHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public int CallCount { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            CallCount++;
            return Task.FromResult(responder(request));
        }
    }

    private sealed class FakeTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}