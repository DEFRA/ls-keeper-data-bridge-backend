using Amazon.S3;
using Amazon.S3.Model;
using System.Text;
using Testcontainers.LocalStack;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

/// <summary>
/// Fixture for sharing LocalStack container across all tests
/// </summary>
public class LocalStackFixture : IAsyncLifetime
{
    public LocalStackContainer Container { get; private set; } = null!;
    public IAmazonS3 S3Client { get; private set; } = null!;

    public const string TestBucket = "test-bucket";

    public async Task InitializeAsync()
    {
        Container = new LocalStackBuilder()
            .WithImage("localstack/localstack:2.3")
            .WithEnvironment("SERVICES", "s3")
            .WithEnvironment("DEBUG", "1")
            .WithEnvironment("AWS_DEFAULT_REGION", "us-east-1")
            .WithEnvironment("AWS_ACCESS_KEY_ID", "test")
            .WithEnvironment("AWS_SECRET_ACCESS_KEY", "test")
            .WithEnvironment("EDGE_PORT", "4566")
            .WithPortBinding(4566, true)
            .Build();

        await Container.StartAsync();

        // Wait for LocalStack to fully start
        //await Task.Delay(2000); // Increased wait time

        var url = Container.GetConnectionString();

        // Create S3 client with proper LocalStack configuration
        var config = new AmazonS3Config
        {
            ServiceURL = url,
            ForcePathStyle = true,
            UseHttp = true,
            MaxErrorRetry = 3,
            Timeout = TimeSpan.FromMinutes(5),
            //RegionEndpoint = Amazon.RegionEndpoint.USEast1,
            RequestChecksumCalculation = Amazon.Runtime.RequestChecksumCalculation.WHEN_REQUIRED,
            ResponseChecksumValidation = Amazon.Runtime.ResponseChecksumValidation.WHEN_REQUIRED,
        };

        // Use consistent credentials
        S3Client = new AmazonS3Client("test", "test", config);

        // Try to create test bucket with more retries
        var maxRetries = 5;
        var retryDelay = 3000;

        for (var i = 0; i < maxRetries; i++)
        {
            try
            {
                await S3Client.PutBucketAsync(new PutBucketRequest
                {
                    BucketName = TestBucket,
                    UseClientRegion = true
                });

                // Verify bucket creation
                var buckets = await S3Client.ListBucketsAsync();
                if (buckets.Buckets.Any(b => b.BucketName == TestBucket))
                {
                    break;
                }
            }
            catch when (i < maxRetries - 1)
            {
                await Task.Delay(retryDelay);
                retryDelay = Math.Min(retryDelay * 2, 10000); // Cap at 10 seconds
            }
        }

        // Setup shared test data
        await SetupTestDataAsync();
    }

    public async Task DisposeAsync()
    {
        try
        {
            S3Client?.Dispose();
        }
        finally
        {
            await Container.DisposeAsync();
        }
    }

    private async Task SetupTestDataAsync()
    {
        try
        {
            // Create simple test files first
            var testFiles = new[]
            {
                ("small.txt", "Small test content"),
                ("medium.txt", string.Join("", Enumerable.Repeat("Medium test content with more data. ", 100))),
                ("subfolder/nested.txt", "Nested file content"),
                ("test-folder/inside-folder.txt", "Content inside top-level folder"),
                ("test-folder/sub/deep.txt", "Deep nested content in folder"),
            };

            foreach (var (key, content) in testFiles)
            {
                var request = new PutObjectRequest
                {
                    BucketName = TestBucket,
                    Key = key,
                    ContentBody = content,
                    ContentType = "text/plain",
                    ServerSideEncryptionMethod = ServerSideEncryptionMethod.None
                };
                request.Metadata["test-meta"] = "test-value";

                await S3Client.PutObjectAsync(request);
            }

            // Create a large file for streaming tests
            await CreateLargeTestFileAsync("large-file.bin");
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to setup test data: {ex.Message}", ex);
        }
    }

    private async Task CreateLargeTestFileAsync(string key)
    {
        try
        {
            // Create a simpler large file with just a single upload (reduced for test efficiency)
            const int fileSize = 10 * 1024 * 1024; // 10MB
            var pattern = Encoding.UTF8.GetBytes("LARGE_FILE_TEST_PATTERN_REPEATED_");
            var data = new byte[fileSize];

            // Fill data with repeating pattern
            for (var i = 0; i < fileSize; i++)
            {
                data[i] = pattern[i % pattern.Length];
            }

            var request = new PutObjectRequest
            {
                BucketName = TestBucket,
                Key = key,
                ContentType = "application/octet-stream",
                InputStream = new MemoryStream(data),
                ServerSideEncryptionMethod = ServerSideEncryptionMethod.None
            };

            await S3Client.PutObjectAsync(request);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to create large test file '{key}': {ex.Message}", ex);
        }
    }
}