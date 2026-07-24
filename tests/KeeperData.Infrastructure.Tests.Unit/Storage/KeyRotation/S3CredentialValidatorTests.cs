using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using KeeperData.Infrastructure.Storage.KeyRotation;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;

namespace KeeperData.Infrastructure.Tests.Unit.Storage.KeyRotation;

public class S3CredentialValidatorTests
{
    private const string BucketName = "cerespfm-dev-dev1-livestockfeeds";
    private const string KeyId = "AKIANEWKEY1234567890";
    private const string Secret = "new-secret-value";

    private readonly Mock<IAmazonS3> _s3ClientMock = new();
    private readonly TestableS3CredentialValidator _sut;

    public S3CredentialValidatorTests()
    {
        _sut = new TestableS3CredentialValidator(_s3ClientMock.Object);
    }

    /// <summary>Substitutes the candidate client so the probe never touches the network.</summary>
    private sealed class TestableS3CredentialValidator(IAmazonS3 candidateClient) : S3CredentialValidator(
        new ExternalStorageS3Config(new AmazonS3Config { RegionEndpoint = Amazon.RegionEndpoint.EUWest2 }),
        Mock.Of<ILogger<S3CredentialValidator>>())
    {
        protected override IAmazonS3 CreateCandidateClient(string accessKeyId, string secretAccessKey) => candidateClient;
    }

    [Fact]
    public async Task Validate_WhenProbeSucceeds_ReturnsValid()
    {
        // Arrange
        _s3ClientMock.Setup(c => c.ListObjectsV2Async(
                It.Is<ListObjectsV2Request>(r => r.BucketName == BucketName && r.MaxKeys == 1),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.OK });

        // Act
        var result = await _sut.ValidateAsync(KeyId, Secret, BucketName);

        // Assert
        result.Outcome.Should().Be(S3CredentialValidationOutcome.Valid);
    }

    [Theory]
    [InlineData("InvalidAccessKeyId", HttpStatusCode.Forbidden)]
    [InlineData("SignatureDoesNotMatch", HttpStatusCode.Forbidden)]
    [InlineData("AccessDenied", HttpStatusCode.Forbidden)]
    [InlineData("ExpiredToken", HttpStatusCode.BadRequest)]
    [InlineData("InvalidSecurity", HttpStatusCode.BadRequest)]
    public async Task Validate_WithDeterministicAuthFailure_ReturnsInvalidCredentials(string errorCode, HttpStatusCode statusCode)
    {
        // Arrange
        _s3ClientMock.Setup(c => c.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonS3Exception("rejected") { ErrorCode = errorCode, StatusCode = statusCode });

        // Act
        var result = await _sut.ValidateAsync(KeyId, Secret, BucketName);

        // Assert
        result.Outcome.Should().Be(S3CredentialValidationOutcome.InvalidCredentials);
        result.Detail.Should().Contain(errorCode);
    }

    [Fact]
    public async Task Validate_With403WithoutKnownErrorCode_ReturnsInvalidCredentials()
    {
        // Arrange
        _s3ClientMock.Setup(c => c.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonS3Exception("forbidden") { StatusCode = HttpStatusCode.Forbidden });

        // Act
        var result = await _sut.ValidateAsync(KeyId, Secret, BucketName);

        // Assert
        result.Outcome.Should().Be(S3CredentialValidationOutcome.InvalidCredentials);
    }

    [Fact]
    public async Task Validate_WithServerError_ReturnsTransientError()
    {
        // Arrange
        _s3ClientMock.Setup(c => c.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonS3Exception("boom") { StatusCode = HttpStatusCode.InternalServerError });

        // Act
        var result = await _sut.ValidateAsync(KeyId, Secret, BucketName);

        // Assert
        result.Outcome.Should().Be(S3CredentialValidationOutcome.TransientError);
    }

    [Fact]
    public async Task Validate_WithNetworkFailure_ReturnsTransientError()
    {
        // Arrange
        _s3ClientMock.Setup(c => c.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new HttpRequestException("connection reset"));

        // Act
        var result = await _sut.ValidateAsync(KeyId, Secret, BucketName);

        // Assert
        result.Outcome.Should().Be(S3CredentialValidationOutcome.TransientError);
        result.Detail.Should().Contain(nameof(HttpRequestException));
    }

    [Theory]
    [InlineData(null, Secret, BucketName)]
    [InlineData(KeyId, "", BucketName)]
    [InlineData(KeyId, Secret, "  ")]
    public async Task Validate_WithMissingArguments_Throws(string? accessKeyId, string secret, string bucket)
    {
        // Act
        var act = () => _sut.ValidateAsync(accessKeyId!, secret, bucket);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>();
    }
}
