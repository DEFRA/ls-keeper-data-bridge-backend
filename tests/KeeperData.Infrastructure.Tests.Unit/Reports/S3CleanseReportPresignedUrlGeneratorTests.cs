using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using KeeperData.Infrastructure.Reports;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Reports;

public class S3CleanseReportPresignedUrlGeneratorTests
{
    private readonly Mock<IAmazonS3> _s3ClientMock;
    private const string TestBucketName = "test-bucket";

    public S3CleanseReportPresignedUrlGeneratorTests()
    {
        _s3ClientMock = new Mock<IAmazonS3>();
    }

    private S3CleanseReportPresignedUrlGenerator CreateGenerator(string? topLevelFolder = null)
    {
        return new S3CleanseReportPresignedUrlGenerator(_s3ClientMock.Object, TestBucketName, topLevelFolder);
    }

    #region GeneratePresignedUrl Tests

    [Fact]
    public void GeneratePresignedUrl_WithDefaultExpiry_Uses7Days()
    {
        // Arrange
        GetPreSignedUrlRequest? capturedRequest = null;
        _s3ClientMock.Setup(s => s.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Callback<GetPreSignedUrlRequest>(req => capturedRequest = req)
            .Returns("https://presigned-url.example.com");

        var generator = CreateGenerator();
        var now = DateTime.UtcNow;

        // Act
        generator.GeneratePresignedUrl("reports/report.zip");

        // Assert
        capturedRequest.Should().NotBeNull();
        var expectedExpiry = now.AddDays(7);
        capturedRequest!.Expires.Should().BeCloseTo(expectedExpiry, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void GeneratePresignedUrl_WithCustomExpiry_UsesProvidedExpiry()
    {
        // Arrange
        GetPreSignedUrlRequest? capturedRequest = null;
        _s3ClientMock.Setup(s => s.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Callback<GetPreSignedUrlRequest>(req => capturedRequest = req)
            .Returns("https://presigned-url.example.com");

        var generator = CreateGenerator();
        var customExpiry = TimeSpan.FromHours(2);
        var now = DateTime.UtcNow;

        // Act
        generator.GeneratePresignedUrl("reports/report.zip", customExpiry);

        // Assert
        capturedRequest.Should().NotBeNull();
        var expectedExpiry = now.Add(customExpiry);
        capturedRequest!.Expires.Should().BeCloseTo(expectedExpiry, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void GeneratePresignedUrl_WithoutTopLevelFolder_UsesKeyAsIs()
    {
        // Arrange
        GetPreSignedUrlRequest? capturedRequest = null;
        _s3ClientMock.Setup(s => s.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Callback<GetPreSignedUrlRequest>(req => capturedRequest = req)
            .Returns("https://presigned-url.example.com");

        var generator = CreateGenerator();

        // Act
        generator.GeneratePresignedUrl("reports/report.zip");

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.Key.Should().Be("reports/report.zip");
        capturedRequest.BucketName.Should().Be(TestBucketName);
    }

    [Fact]
    public void GeneratePresignedUrl_WithTopLevelFolder_PrependsFolder()
    {
        // Arrange
        GetPreSignedUrlRequest? capturedRequest = null;
        _s3ClientMock.Setup(s => s.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Callback<GetPreSignedUrlRequest>(req => capturedRequest = req)
            .Returns("https://presigned-url.example.com");

        var generator = CreateGenerator("cleanse-exports");

        // Act
        generator.GeneratePresignedUrl("reports/report.zip");

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.Key.Should().Be("cleanse-exports/reports/report.zip");
    }

    [Fact]
    public void GeneratePresignedUrl_WithTopLevelFolderWithTrailingSlash_NormalizesFolder()
    {
        // Arrange
        GetPreSignedUrlRequest? capturedRequest = null;
        _s3ClientMock.Setup(s => s.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Callback<GetPreSignedUrlRequest>(req => capturedRequest = req)
            .Returns("https://presigned-url.example.com");

        var generator = CreateGenerator("cleanse-exports/");

        // Act
        generator.GeneratePresignedUrl("reports/report.zip");

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.Key.Should().Be("cleanse-exports/reports/report.zip");
    }

    [Fact]
    public void GeneratePresignedUrl_WithObjectKeyWithLeadingSlash_TrimsLeadingSlash()
    {
        // Arrange
        GetPreSignedUrlRequest? capturedRequest = null;
        _s3ClientMock.Setup(s => s.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Callback<GetPreSignedUrlRequest>(req => capturedRequest = req)
            .Returns("https://presigned-url.example.com");

        var generator = CreateGenerator("cleanse-exports");

        // Act
        generator.GeneratePresignedUrl("/reports/report.zip");

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.Key.Should().Be("cleanse-exports/reports/report.zip");
    }

    [Fact]
    public void GeneratePresignedUrl_SetsCorrectHttpVerb()
    {
        // Arrange
        GetPreSignedUrlRequest? capturedRequest = null;
        _s3ClientMock.Setup(s => s.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Callback<GetPreSignedUrlRequest>(req => capturedRequest = req)
            .Returns("https://presigned-url.example.com");

        var generator = CreateGenerator();

        // Act
        generator.GeneratePresignedUrl("report.zip");

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.Verb.Should().Be(HttpVerb.GET);
    }

    [Fact]
    public void GeneratePresignedUrl_ReturnsUrlFromS3Client()
    {
        // Arrange
        const string expectedUrl = "https://test-bucket.s3.amazonaws.com/report.zip?signature=abc123";
        _s3ClientMock.Setup(s => s.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Returns(expectedUrl);

        var generator = CreateGenerator();

        // Act
        var result = generator.GeneratePresignedUrl("report.zip");

        // Assert
        result.Should().Be(expectedUrl);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void GeneratePresignedUrl_WithNullOrEmptyTopLevelFolder_UsesKeyDirectly(string? topLevelFolder)
    {
        // Arrange
        GetPreSignedUrlRequest? capturedRequest = null;
        _s3ClientMock.Setup(s => s.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Callback<GetPreSignedUrlRequest>(req => capturedRequest = req)
            .Returns("https://presigned-url.example.com");

        var generator = CreateGenerator(topLevelFolder);

        // Act
        generator.GeneratePresignedUrl("my-report.zip");

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.Key.Should().Be("my-report.zip");
    }

    [Fact]
    public void GeneratePresignedUrl_WithTopLevelFolderWithMultipleTrailingSlashes_NormalizesCorrectly()
    {
        // Arrange
        GetPreSignedUrlRequest? capturedRequest = null;
        _s3ClientMock.Setup(s => s.GetPreSignedURL(It.IsAny<GetPreSignedUrlRequest>()))
            .Callback<GetPreSignedUrlRequest>(req => capturedRequest = req)
            .Returns("https://presigned-url.example.com");

        var generator = CreateGenerator("exports///");

        // Act
        generator.GeneratePresignedUrl("report.zip");

        // Assert
        capturedRequest.Should().NotBeNull();
        // The implementation trims trailing slashes and adds one back
        capturedRequest!.Key.Should().Be("exports/report.zip");
    }

    #endregion
}
