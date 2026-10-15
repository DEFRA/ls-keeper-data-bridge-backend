using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using FluentAssertions;
using KeeperData.Bridge.Authentication;
using KeeperData.Bridge.Config;
using KeeperData.Bridge.Tests.Integration.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace KeeperData.Bridge.Tests.Integration.Authentication;

[Collection("LocalStack")]
[Trait("Dependence", "docker")]
public class StsLocalStackIntegrationTests(LocalStackFixture localStack)
{
    private readonly LocalStackFixture _localStack = localStack;

    // LocalStack ignores the credentials in a presigned query string and always answers GetCallerIdentity
    // as the account root, so assumed-role ARN parsing is covered by the unit tests instead.
    [Fact]
    public async Task VerifyAsync_AgainstLocalStackSts_WhenAllowlisted_ShouldSucceed()
    {
        var endpoint = _localStack.Container.GetConnectionString().TrimEnd('/');

        var presignedUrl = PresignGetCallerIdentity(
            endpointUri: new Uri(endpoint),
            region: "us-east-1",
            accessKeyId: "test",
            secretAccessKey: "test",
            sessionToken: null,
            utcNow: DateTimeOffset.UtcNow,
            expiresSeconds: 60);

        var verifier = CreateVerifier(endpoint, new AllowedStsCaller
        {
            AccountId = "000000000000",
            RolePattern = "root"
        });

        var result = await verifier.VerifyAsync(presignedUrl);

        result.Succeeded.Should().BeTrue(result.FailureReason);
        result.Identity.Should().NotBeNull();
        result.Identity!.Account.Should().Be("000000000000");
        result.Identity.Arn.Should().Be("arn:aws:iam::000000000000:root");
        result.Identity.RoleName.Should().Be("root");
    }

    [Fact]
    public async Task VerifyAsync_AgainstLocalStackSts_WhenCallerRoleNotAllowlisted_ShouldReject()
    {
        var endpoint = _localStack.Container.GetConnectionString().TrimEnd('/');

        var presignedUrl = PresignGetCallerIdentity(
            endpointUri: new Uri(endpoint),
            region: "us-east-1",
            accessKeyId: "test",
            secretAccessKey: "test",
            sessionToken: null,
            utcNow: DateTimeOffset.UtcNow,
            expiresSeconds: 60);

        var verifier = CreateVerifier(endpoint, new AllowedStsCaller
        {
            AccountId = "000000000000",
            RolePattern = "ls-keeper-data-api"
        });

        var result = await verifier.VerifyAsync(presignedUrl);

        result.Succeeded.Should().BeFalse();
        result.FailureReason.Should().Be("Caller identity is not on the allowlist");
    }

    private static StsIdentityVerifier CreateVerifier(string endpoint, AllowedStsCaller allowedCaller)
    {
        var options = new ApiAuthenticationOptions
        {
            Sts = new StsAuthenticationOptions
            {
                Enabled = true,
                Endpoint = endpoint,
                AllowedCallers = [allowedCaller]
            }
        };

        var monitor = new Mock<IOptionsMonitor<ApiAuthenticationOptions>>();
        monitor.Setup(m => m.CurrentValue).Returns(options);

        var factory = new Mock<IHttpClientFactory>();
        factory.Setup(f => f.CreateClient(StsIdentityVerifier.HttpClientName))
            .Returns(() => new HttpClient());

        return new StsIdentityVerifier(factory.Object, monitor.Object, NullLogger<StsIdentityVerifier>.Instance);
    }

    private static string PresignGetCallerIdentity(
        Uri endpointUri,
        string region,
        string accessKeyId,
        string secretAccessKey,
        string? sessionToken,
        DateTimeOffset utcNow,
        int expiresSeconds)
    {
        var amzDate = utcNow.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var dateStamp = utcNow.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        var credentialScope = $"{dateStamp}/{region}/sts/aws4_request";
        var hostHeader = endpointUri.IsDefaultPort ? endpointUri.Host : $"{endpointUri.Host}:{endpointUri.Port}";

        var queryParams = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["Action"] = "GetCallerIdentity",
            ["Version"] = "2011-06-15",
            ["X-Amz-Algorithm"] = "AWS4-HMAC-SHA256",
            ["X-Amz-Credential"] = $"{accessKeyId}/{credentialScope}",
            ["X-Amz-Date"] = amzDate,
            ["X-Amz-Expires"] = expiresSeconds.ToString(CultureInfo.InvariantCulture),
            ["X-Amz-SignedHeaders"] = "host"
        };

        if (!string.IsNullOrEmpty(sessionToken))
        {
            queryParams["X-Amz-Security-Token"] = sessionToken;
        }

        var canonicalQuery = string.Join("&", queryParams.Select(kvp =>
            $"{Uri.EscapeDataString(kvp.Key)}={Uri.EscapeDataString(kvp.Value)}"));

        var canonicalHeaders = $"host:{hostHeader}\n";
        const string payloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        var canonicalRequest = $"GET\n/\n{canonicalQuery}\n{canonicalHeaders}\nhost\n{payloadHash}";

        var stringToSign = $"AWS4-HMAC-SHA256\n{amzDate}\n{credentialScope}\n{Sha256Hex(canonicalRequest)}";
        var signingKey = DeriveSigningKey(secretAccessKey, dateStamp, region, "sts");
        var signature = Convert.ToHexStringLower(HMACSHA256.HashData(signingKey, Encoding.UTF8.GetBytes(stringToSign)));

        return $"{endpointUri.Scheme}://{hostHeader}/?{canonicalQuery}&X-Amz-Signature={signature}";
    }

    private static string Sha256Hex(string value)
        => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));

    private static byte[] DeriveSigningKey(string secretAccessKey, string dateStamp, string region, string service)
    {
        var kDate = HMACSHA256.HashData(Encoding.UTF8.GetBytes("AWS4" + secretAccessKey), Encoding.UTF8.GetBytes(dateStamp));
        var kRegion = HMACSHA256.HashData(kDate, Encoding.UTF8.GetBytes(region));
        var kService = HMACSHA256.HashData(kRegion, Encoding.UTF8.GetBytes(service));
        return HMACSHA256.HashData(kService, Encoding.UTF8.GetBytes("aws4_request"));
    }
}