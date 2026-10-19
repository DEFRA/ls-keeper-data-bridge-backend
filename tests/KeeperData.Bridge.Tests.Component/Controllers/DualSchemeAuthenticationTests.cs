using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using FluentAssertions;
using KeeperData.Bridge.Authentication;
using KeeperData.Infrastructure.Storage;
using Microsoft.AspNetCore.TestHost;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;

namespace KeeperData.Bridge.Tests.Component.Controllers;

public class DualSchemeAuthenticationTests(AppWebApplicationFactory factory) : IClassFixture<AppWebApplicationFactory>
{
    private const string TestApiKey = "test-dual-scheme-api-key";
    private const string SamplePresignedUrl =
        "https://sts.eu-west-2.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15";

    private readonly AppWebApplicationFactory _factory = factory;

    private (HttpClient Client, Mock<IStsIdentityVerifier> VerifierMock) CreateAuthenticatedClient()
    {
        var verifierMock = new Mock<IStsIdentityVerifier>();
        var customFactory = _factory.WithWebHostBuilder(builder =>
        {
            builder.ConfigureAppConfiguration((_, config) =>
            {
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["FeatureFlags:AuthenticationEnabled"] = "true",
                    ["ApiAuthentication:ApiKey"] = TestApiKey,
                    ["ApiAuthentication:ApiKeyEnabled"] = "true",
                    ["ApiAuthentication:Sts:Enabled"] = "true"
                });
            });

            builder.ConfigureTestServices(services =>
            {
                services.Replace(ServiceDescriptor.Singleton(verifierMock.Object));
            });
        });

        return (customFactory.CreateClient(), verifierMock);
    }

    [Fact]
    public async Task ProtectedEndpoint_WithValidAwsStsHeader_ShouldReturnOk()
    {
        var (client, verifierMock) = CreateAuthenticatedClient();
        var identity = new StsCallerIdentity(
            "arn:aws:sts::332499610595:assumed-role/ls-keeper-data-api/session",
            "332499610595",
            "AROAEXAMPLE:session",
            "ls-keeper-data-api");

        verifierMock
            .Setup(v => v.VerifyAsync(SamplePresignedUrl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(StsVerificationResult.Success(identity));

        var token = WebEncoders.Base64UrlEncode(Encoding.UTF8.GetBytes(SamplePresignedUrl));
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("AwsSts", token);

        var response = await client.GetAsync($"/api/externalcatalogue/files?sourceType={BlobStorageSources.Internal}");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ProtectedEndpoint_WithValidApiKeyHeader_ShouldStillReturnOkWhenStsIsEnabled()
    {
        var (client, verifierMock) = CreateAuthenticatedClient();
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("ApiKey", TestApiKey);

        var response = await client.GetAsync($"/api/externalcatalogue/files?sourceType={BlobStorageSources.Internal}");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        verifierMock.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ProtectedEndpoint_WithRejectedAwsStsHeader_ShouldReturnUnauthorized()
    {
        var (client, verifierMock) = CreateAuthenticatedClient();
        verifierMock
            .Setup(v => v.VerifyAsync(SamplePresignedUrl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(StsVerificationResult.Fail("Caller identity is not on the allowlist"));

        var token = WebEncoders.Base64UrlEncode(Encoding.UTF8.GetBytes(SamplePresignedUrl));
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("AwsSts", token);

        var response = await client.GetAsync($"/api/externalcatalogue/files?sourceType={BlobStorageSources.Internal}");

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task ProtectedEndpoint_WithNoAuthorizationHeader_ShouldReturnUnauthorized()
    {
        var (client, _) = CreateAuthenticatedClient();

        var response = await client.GetAsync($"/api/externalcatalogue/files?sourceType={BlobStorageSources.Internal}");

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task ProtectedEndpoint_EndToEndThroughRealStsIdentityVerifier_ShouldValidateAndAllowCaller()
    {
        const string stsResponseXml = """
            <GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
              <GetCallerIdentityResult>
                <Arn>arn:aws:sts::332499610595:assumed-role/ls-keeper-data-api/task-01</Arn>
                <UserId>AROAEXAMPLE:task-01</UserId>
                <Account>332499610595</Account>
              </GetCallerIdentityResult>
            </GetCallerIdentityResponse>
            """;

        var fakeStsHandler = new StubHttpMessageHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(stsResponseXml, Encoding.UTF8, "text/xml")
        });

        var customFactory = _factory.WithWebHostBuilder(builder =>
        {
            builder.ConfigureAppConfiguration((_, config) =>
            {
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["FeatureFlags:AuthenticationEnabled"] = "true",
                    ["ApiAuthentication:ApiKey"] = TestApiKey,
                    ["ApiAuthentication:ApiKeyEnabled"] = "true",
                    ["ApiAuthentication:Sts:Enabled"] = "true",
                    ["ApiAuthentication:Sts:Endpoint"] = "https://sts.eu-west-2.amazonaws.com",
                    ["ApiAuthentication:Sts:AllowedCallers:0:AccountId"] = "332499610595",
                    ["ApiAuthentication:Sts:AllowedCallers:0:RolePattern"] = "ls-keeper-data-*"
                });
            });

            builder.ConfigureTestServices(services =>
            {
                services.AddHttpClient(StsIdentityVerifier.HttpClientName)
                    .ConfigurePrimaryHttpMessageHandler(() => fakeStsHandler);
            });
        });

        var client = customFactory.CreateClient();
        var amzDate = DateTimeOffset.UtcNow.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var presignedUrl =
            "https://sts.eu-west-2.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15"
            + "&X-Amz-Algorithm=AWS4-HMAC-SHA256"
            + "&X-Amz-Credential=AKIAEXAMPLE%2F20261014%2Feu-west-2%2Fsts%2Faws4_request"
            + $"&X-Amz-Date={amzDate}"
            + "&X-Amz-Expires=60"
            + "&X-Amz-SignedHeaders=host"
            + "&X-Amz-Signature=deadbeef";

        var token = WebEncoders.Base64UrlEncode(Encoding.UTF8.GetBytes(presignedUrl));
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("AwsSts", token);

        var response = await client.GetAsync($"/api/externalcatalogue/files?sourceType={BlobStorageSources.Internal}");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        fakeStsHandler.CallCount.Should().Be(1);
    }

    private sealed class StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> handler) : HttpMessageHandler
    {
        public int CallCount { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            CallCount++;
            return Task.FromResult(handler(request));
        }
    }
}