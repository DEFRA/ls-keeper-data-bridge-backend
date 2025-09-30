using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Crypto;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Logging;
using Moq;
using System.Security.Cryptography;
using System.Text;
using Xunit.Abstractions;
using Amazon.S3;
using KeeperData.Core.Storage;

namespace KeeperData.Bridge.Tests.Integration.Scenarios;

[Collection("LocalStack")]
public class ScenarioFileDecryptTransferTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly LocalStackFixture _localStackFixture;
    private readonly Mock<ILogger<BlobStorageService>> _loggerMock;
    private readonly BlobStorageService _sourceStorageService;
    private readonly BlobStorageService _destinationStorageService;
    private readonly IAesCryptoTransform _cryptoTransform;
    private readonly TestScope _testScope;

    private const string TestPassword = "testtest123";
    private const string TestSalt = "Jr8Lm2PXzd7qNbVyWutRfGBxhkHTpE";
    private const string SourceFolder = "encrypted-source";
    private const string DestinationFolder = "decrypted-destination";

    public ScenarioFileDecryptTransferTests(ITestOutputHelper testOutputHelper, LocalStackFixture localStackFixture)
    {
        _testOutputHelper = testOutputHelper;
        _localStackFixture = localStackFixture;
        _loggerMock = new Mock<ILogger<BlobStorageService>>();

        _sourceStorageService = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket, SourceFolder);
        _destinationStorageService = new BlobStorageService(_localStackFixture.S3Client, _loggerMock.Object, LocalStackFixture.TestBucket, DestinationFolder);

        _cryptoTransform = new AesCryptoTransform();

        _testScope = new TestScope(_sourceStorageService, _destinationStorageService, LocalStackFixture.TestBucket);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _testScope.DisposeAsync();
        _sourceStorageService?.Dispose();
        _destinationStorageService?.Dispose();
    }


    [Fact]
    public async Task StreamDecryptTransfer_WithProgressReporting_ShouldReportProgress()
    {
        _testOutputHelper.WriteLine("Testing progress reporting during stream decrypt transfer");

        _testScope.TrackForCleanup("progress-test.csv");
        _testScope.TrackForCleanup("progress-test-encrypted.csv");
        _testScope.TrackForCleanup("progress-test-decrypted.csv");

        const long smallerFileSize = 2L * 1024L * 1024L;
        var progressReports = new List<(int percentage, string status)>();

        ProgressCallback progressCallback = (percentage, status) =>
        {
            progressReports.Add((percentage, status));
            _testOutputHelper.WriteLine($"Progress: {percentage}% - {status}");
        };

        await CreateTestCsvFileAsync("progress-test.csv", smallerFileSize);

        await using (var sourceStream = await _sourceStorageService.OpenReadAsync("progress-test.csv"))
        await using (var encryptedUploadStream = await _sourceStorageService.OpenWriteAsync("progress-test-encrypted.csv", "application/octet-stream"))
        {
            await _cryptoTransform.EncryptStreamAsync(sourceStream, encryptedUploadStream, TestPassword, TestSalt,
                smallerFileSize, progressCallback);
        }

        progressReports.Clear();
        await using (var encryptedStream = await _sourceStorageService.OpenReadAsync("progress-test-encrypted.csv"))
        await using (var decryptedUploadStream = await _destinationStorageService.OpenWriteAsync("progress-test-decrypted.csv", "text/csv"))
        {
            var encryptedMetadata = await _sourceStorageService.GetMetadataAsync("progress-test-encrypted.csv");

            await _cryptoTransform.DecryptStreamAsync(encryptedStream, decryptedUploadStream, TestPassword, TestSalt,
                encryptedMetadata.ContentLength, progressCallback);
        }

        progressReports.Should().NotBeEmpty("Progress callback should have been invoked during decryption");
        progressReports.Should().Contain(p => p.percentage == 100, "Progress should reach 100% completion");
        progressReports.Should().Contain(p => p.status.Contains("Decrypting"), "Progress status should indicate decryption operation");

        _testOutputHelper.WriteLine($"✅ Progress reporting test completed. Total progress reports: {progressReports.Count}");
    }

    [Fact]
    public async Task StreamDecryptTransfer_SmallFiles_ShouldHandleCorrectly()
    {
        _testOutputHelper.WriteLine("Testing stream decrypt transfer with small files");

        const long fileSize = 1L * 1024L * 1024L;
        const int numberOfFiles = 2;
        var results = new List<string>();

        for (int i = 0; i < numberOfFiles; i++)
        {
            var csvFile = $"small-test-{i}.csv";
            var encryptedFile = $"small-test-{i}-encrypted.csv";
            var decryptedFile = $"small-test-{i}-decrypted.csv";

            _testScope.TrackForCleanup(csvFile);
            _testScope.TrackForCleanup(encryptedFile);
            _testScope.TrackForCleanup(decryptedFile);

            await CreateTestCsvFileAsync(csvFile, fileSize);
            var originalMd5 = await DownloadAndCalculateMd5Async(_sourceStorageService, csvFile);

            await using (var sourceStream = await _sourceStorageService.OpenReadAsync(csvFile))
            await using (var encryptedStream = await _sourceStorageService.OpenWriteAsync(encryptedFile, "application/octet-stream"))
            {
                await _cryptoTransform.EncryptStreamAsync(sourceStream, encryptedStream, TestPassword, TestSalt, fileSize);
            }

            await using (var encryptedReadStream = await _sourceStorageService.OpenReadAsync(encryptedFile))
            await using (var decryptedStream = await _destinationStorageService.OpenWriteAsync(decryptedFile, "text/csv"))
            {
                var encryptedMetadata = await _sourceStorageService.GetMetadataAsync(encryptedFile);
                await _cryptoTransform.DecryptStreamAsync(encryptedReadStream, decryptedStream, TestPassword, TestSalt, encryptedMetadata.ContentLength);
            }

            var finalMd5 = await DownloadAndCalculateMd5Async(_destinationStorageService, decryptedFile);
            finalMd5.Should().Be(originalMd5, $"File {i} should maintain integrity through encrypt/decrypt process");

            results.Add(finalMd5);
            _testOutputHelper.WriteLine($"File {i} processed successfully with MD5: {originalMd5}");
        }

        results.Should().HaveCount(numberOfFiles);
        results.Should().AllSatisfy(result => result.Should().NotBeNullOrEmpty("Each file operation should return a valid MD5 hash"));

        _testOutputHelper.WriteLine($"✅ Small file processing completed successfully. Processed {numberOfFiles} files.");
    }

    [Theory]
    [InlineData(1L * 1024L * 1024L)]
    [InlineData(5L * 1024L * 1024L)]
    [InlineData(10L * 1024L * 1024L)]
    [InlineData(25L * 1024L * 1024L)]
    [InlineData(500L * 1024L * 1024L)]
    public async Task StreamDecryptTransfer_ShouldMaintainIntegrity(long fileSizeBytes)
    {
        _testOutputHelper.WriteLine($"Starting local filesystem to S3 to local filesystem test with {fileSizeBytes:N0} bytes file");

        var testId = Guid.NewGuid().ToString("N")[..8];
        var originalFile = $"original-{testId}.csv";
        var encryptedFile = $"encrypted-{testId}.csv";
        var s3EncryptedKey = $"s3-encrypted-{testId}.csv";
        var s3DecryptedKey = $"s3-decrypted-{testId}.csv";
        var finalDecryptedFile = $"final-{testId}.csv";

        _testScope.TrackForCleanup(s3EncryptedKey);
        _testScope.TrackForCleanup(s3DecryptedKey);

        var tempDir = Path.GetTempPath();
        var originalFilePath = Path.Combine(tempDir, originalFile);
        var encryptedFilePath = Path.Combine(tempDir, encryptedFile);
        var finalDecryptedFilePath = Path.Combine(tempDir, finalDecryptedFile);

        var localFilesToCleanup = new[] { originalFilePath, encryptedFilePath, finalDecryptedFilePath };

        try
        {
            _testOutputHelper.WriteLine("Step 1: Creating original CSV test file on local filesystem...");
            var originalMd5Hash = await CreateLocalTestCsvFileAsync(originalFilePath, fileSizeBytes);
            _testOutputHelper.WriteLine($"Original file created: {originalFilePath}, MD5: {originalMd5Hash}");

            _testOutputHelper.WriteLine("Step 2: Encrypting file locally...");
            await EncryptLocalFileAsync(originalFilePath, encryptedFilePath, fileSizeBytes);
            _testOutputHelper.WriteLine($"Encrypted file created: {encryptedFilePath}");

            _testOutputHelper.WriteLine("Step 3: Streaming encrypted file to S3...");
            await StreamUploadToS3Async(encryptedFilePath, s3EncryptedKey);
            _testOutputHelper.WriteLine($"Encrypted file uploaded to S3: {s3EncryptedKey}");

            _testOutputHelper.WriteLine("Step 4: Streaming from S3, decrypting on-the-fly, and uploading decrypted to S3...");
            await StreamDecryptFromS3ToS3Async(s3EncryptedKey, s3DecryptedKey);
            _testOutputHelper.WriteLine($"Decrypted file uploaded to S3: {s3DecryptedKey}");

            _testOutputHelper.WriteLine("Step 5: Streaming decrypted file from S3 to local filesystem...");
            await StreamDownloadFromS3Async(s3DecryptedKey, finalDecryptedFilePath);
            _testOutputHelper.WriteLine($"Final decrypted file downloaded: {finalDecryptedFilePath}");

            _testOutputHelper.WriteLine("Step 6: Verifying MD5 integrity...");
            var finalMd5Hash = await CalculateLocalFileMd5Async(finalDecryptedFilePath);

            finalMd5Hash.Should().Be(originalMd5Hash,
                $"The MD5 hash of the final decrypted file should match the original file for {fileSizeBytes:N0} bytes, proving data integrity throughout the entire encrypt/decrypt/transfer process");

            _testOutputHelper.WriteLine($"✅ Test completed successfully for {fileSizeBytes:N0} bytes! MD5 integrity verified: {originalMd5Hash}");

            var originalSize = new FileInfo(originalFilePath).Length;
            var encryptedSize = new FileInfo(encryptedFilePath).Length;
            var finalSize = new FileInfo(finalDecryptedFilePath).Length;

            _testOutputHelper.WriteLine($"File size analysis:");
            _testOutputHelper.WriteLine($"  Original: {originalSize:N0} bytes");
            _testOutputHelper.WriteLine($"  Encrypted: {encryptedSize:N0} bytes ({(double)encryptedSize / originalSize:P1} of original)");
            _testOutputHelper.WriteLine($"  Final: {finalSize:N0} bytes");
        }
        finally
        {
            foreach (var filePath in localFilesToCleanup)
            {
                try
                {
                    if (File.Exists(filePath))
                    {
                        File.Delete(filePath);
                        _testOutputHelper.WriteLine($"Cleaned up local file: {filePath}");
                    }
                }
                catch (Exception ex)
                {
                    _testOutputHelper.WriteLine($"Warning: Failed to clean up local file {filePath}: {ex.Message}");
                }
            }
        }
    }

    private async Task<string> CreateLocalTestCsvFileAsync(string filePath, long targetSizeBytes)
    {
        _testOutputHelper.WriteLine($"Creating local CSV test file: {filePath} with target size: {targetSizeBytes:N0} bytes");

        await using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 64 * 1024))
        await using (var writer = new StreamWriter(fileStream, Encoding.UTF8))
        {
            await writer.WriteLineAsync("Id,Name,Email,Department,Salary,StartDate,IsActive,Notes");

            var random = new Random(42);
            var departments = new[] { "Engineering", "Marketing", "Sales", "HR", "Finance", "Operations", "Support" };
            var domains = new[] { "example.com", "test.org", "company.net", "business.co.uk" };

            long bytesWritten = 0;
            int recordId = 1;

            while (bytesWritten < targetSizeBytes)
            {
                var name = $"{GenerateRandomName(random)} {GenerateRandomName(random)}";
                var email = $"{name.Replace(" ", ".").ToLower()}@{domains[random.Next(domains.Length)]}";
                var department = departments[random.Next(departments.Length)];
                var salary = random.Next(30000, 150000);
                var startDate = DateTime.Now.AddDays(-random.Next(1, 3650)).ToString("yyyy-MM-dd");
                var isActive = random.Next(0, 2) == 1 ? "true" : "false";
                var notes = $"Employee record {recordId} with additional notes and description data to increase row size for testing purposes";

                var csvRow = $"{recordId},{name},{email},{department},{salary},{startDate},{isActive},\"{notes}\"";
                await writer.WriteLineAsync(csvRow);

                bytesWritten = writer.BaseStream.Position;

                if (recordId % 10000 == 0)
                {
                    _testOutputHelper.WriteLine($"Generated {recordId:N0} records, ~{bytesWritten:N0} bytes ({(double)bytesWritten / targetSizeBytes * 100:F1}%)");
                }

                recordId++;
            }

            await writer.FlushAsync();
            await fileStream.FlushAsync();

            _testOutputHelper.WriteLine($"✅ Created local CSV file with {recordId - 1:N0} records");
        }

        var actualSize = new FileInfo(filePath).Length;
        _testOutputHelper.WriteLine($"Actual file size: {actualSize:N0} bytes");

        return await CalculateLocalFileMd5Async(filePath);
    }

    private async Task<string> CalculateLocalFileMd5Async(string filePath)
    {
        _testOutputHelper.WriteLine($"Calculating MD5 hash for local file: {filePath}");

        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var md5 = MD5.Create();

        var hashBytes = await md5.ComputeHashAsync(fileStream);
        var hashString = Convert.ToHexString(hashBytes);

        _testOutputHelper.WriteLine($"MD5 hash for {filePath}: {hashString}");
        return hashString;
    }

    private async Task EncryptLocalFileAsync(string inputFilePath, string outputFilePath, long estimatedSize)
    {
        _testOutputHelper.WriteLine($"Encrypting local file from {inputFilePath} to {outputFilePath}");

        var encryptProgress = 0;
        ProgressCallback encryptProgressCallback = (percentage, status) =>
        {
            if (percentage != encryptProgress && percentage % 20 == 0)
            {
                _testOutputHelper.WriteLine($"Local encryption: {percentage}% - {status}");
                encryptProgress = percentage;
            }
        };

        await _cryptoTransform.EncryptFileAsync(inputFilePath, outputFilePath, TestPassword, TestSalt,
            encryptProgressCallback);

        var encryptedSize = new FileInfo(outputFilePath).Length;
        _testOutputHelper.WriteLine($"✅ Local file encrypted. Size: {encryptedSize:N0} bytes");
    }

    private async Task StreamUploadToS3Async(string localFilePath, string s3Key)
    {
        _testOutputHelper.WriteLine($"Streaming upload from {localFilePath} to S3 key: {s3Key}");

        await using var localFileStream = new FileStream(localFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        await using var s3UploadStream = await _sourceStorageService.OpenWriteAsync(s3Key, "application/octet-stream");

        var totalBytes = localFileStream.Length;
        var buffer = new byte[64 * 1024];
        long totalUploaded = 0;
        int bytesRead;

        while ((bytesRead = await localFileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
        {
            await s3UploadStream.WriteAsync(buffer, 0, bytesRead);
            totalUploaded += bytesRead;

            if (totalUploaded % (1024 * 1024) == 0)
            {
                var percentage = (int)((double)totalUploaded / totalBytes * 100);
                _testOutputHelper.WriteLine($"S3 upload progress: {percentage}% ({totalUploaded:N0} / {totalBytes:N0} bytes)");
            }
        }

        await s3UploadStream.FlushAsync();
        await s3UploadStream.DisposeAsync();

        await Task.Delay(100);
        var exists = await _sourceStorageService.ExistsAsync(s3Key);
        if (!exists)
        {
            throw new InvalidOperationException($"S3 upload verification failed for key: {s3Key}");
        }

        var metadata = await _sourceStorageService.GetMetadataAsync(s3Key);
        _testOutputHelper.WriteLine($"✅ S3 upload completed. S3 size: {metadata.ContentLength:N0} bytes");
    }

    private async Task StreamDecryptFromS3ToS3Async(string encryptedS3Key, string decryptedS3Key)
    {
        _testOutputHelper.WriteLine($"Streaming decrypt from S3 key {encryptedS3Key} to S3 key {decryptedS3Key}");

        await using var encryptedS3Stream = await _sourceStorageService.OpenReadAsync(encryptedS3Key);
        await using var decryptedS3UploadStream = await _destinationStorageService.OpenWriteAsync(decryptedS3Key, "text/csv");

        var encryptedMetadata = await _sourceStorageService.GetMetadataAsync(encryptedS3Key);

        var decryptProgress = 0;
        ProgressCallback decryptProgressCallback = (percentage, status) =>
        {
            if (percentage != decryptProgress && percentage % 20 == 0)
            {
                _testOutputHelper.WriteLine($"S3 to S3 decryption: {percentage}% - {status}");
                decryptProgress = percentage;
            }
        };

        await _cryptoTransform.DecryptStreamAsync(encryptedS3Stream, decryptedS3UploadStream, TestPassword, TestSalt,
            encryptedMetadata.ContentLength, decryptProgressCallback);

        await decryptedS3UploadStream.FlushAsync();
        await decryptedS3UploadStream.DisposeAsync();

        await Task.Delay(100);
        var exists = await _destinationStorageService.ExistsAsync(decryptedS3Key);
        if (!exists)
        {
            throw new InvalidOperationException($"S3 decryption upload verification failed for key: {decryptedS3Key}");
        }

        var decryptedMetadata = await _destinationStorageService.GetMetadataAsync(decryptedS3Key);
        _testOutputHelper.WriteLine($"✅ S3 to S3 decryption completed. Decrypted size: {decryptedMetadata.ContentLength:N0} bytes");
    }

    private async Task StreamDownloadFromS3Async(string s3Key, string localFilePath)
    {
        _testOutputHelper.WriteLine($"Streaming download from S3 key {s3Key} to {localFilePath}");

        await using var s3Stream = await _destinationStorageService.OpenReadAsync(s3Key);
        await using var localFileStream = new FileStream(localFilePath, FileMode.Create, FileAccess.Write, FileShare.None);

        var metadata = await _destinationStorageService.GetMetadataAsync(s3Key);
        var totalBytes = metadata.ContentLength;
        var buffer = new byte[64 * 1024];
        long totalDownloaded = 0;
        int bytesRead;

        while ((bytesRead = await s3Stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
        {
            await localFileStream.WriteAsync(buffer, 0, bytesRead);
            totalDownloaded += bytesRead;

            if (totalDownloaded % (1024 * 1024) == 0)
            {
                var percentage = (int)((double)totalDownloaded / totalBytes * 100);
                _testOutputHelper.WriteLine($"S3 download progress: {percentage}% ({totalDownloaded:N0} / {totalBytes:N0} bytes)");
            }
        }

        await localFileStream.FlushAsync();
        await localFileStream.DisposeAsync();

        var finalSize = new FileInfo(localFilePath).Length;
        _testOutputHelper.WriteLine($"✅ S3 download completed. Local file size: {finalSize:N0} bytes");
    }

    private async Task CreateTestCsvFileAsync(string fileName, long targetSizeBytes)
    {
        _testOutputHelper.WriteLine($"Creating CSV test file: {fileName} with target size: {targetSizeBytes:N0} bytes");

        var csv = new StringBuilder();
        csv.AppendLine("Id,Name,Email,Department,Salary,StartDate,IsActive,Notes");

        var random = new Random(42);
        var departments = new[] { "Engineering", "Marketing", "Sales", "HR", "Finance", "Operations", "Support" };
        var domains = new[] { "example.com", "test.org", "company.net", "business.co.uk" };

        long bytesWritten = 0;
        int recordId = 1;

        while (bytesWritten < targetSizeBytes)
        {
            var name = $"{GenerateRandomName(random)} {GenerateRandomName(random)}";
            var email = $"{name.Replace(" ", ".").ToLower()}@{domains[random.Next(domains.Length)]}";
            var department = departments[random.Next(departments.Length)];
            var salary = random.Next(30000, 150000);
            var startDate = DateTime.Now.AddDays(-random.Next(1, 3650)).ToString("yyyy-MM-dd");
            var isActive = random.Next(0, 2) == 1 ? "true" : "false";
            var notes = $"Employee record {recordId} with additional notes and description data to increase row size";

            var csvRow = $"{recordId},{name},{email},{department},{salary},{startDate},{isActive},\"{notes}\"";

            csv.AppendLine(csvRow);
            bytesWritten = Encoding.UTF8.GetByteCount(csv.ToString());

            if (recordId % 10000 == 0)
            {
                _testOutputHelper.WriteLine($"Generated {recordId:N0} records, ~{bytesWritten:N0} bytes ({(double)bytesWritten / targetSizeBytes * 100:F1}%)");
            }

            recordId++;
        }

        var csvBytes = Encoding.UTF8.GetBytes(csv.ToString());

        await using (var contentStream = new MemoryStream(csvBytes))
        await using (var uploadStream = await _sourceStorageService.OpenWriteAsync(fileName, "text/csv"))
        {
            await contentStream.CopyToAsync(uploadStream);
        }

        _testOutputHelper.WriteLine($"✅ Created and uploaded CSV file using streaming API with {recordId - 1:N0} records, {csvBytes.Length:N0} bytes");

        await Task.Delay(100);

        var exists = await _sourceStorageService.ExistsAsync(fileName);
        if (!exists)
        {
            await Task.Delay(1000);
            exists = await _sourceStorageService.ExistsAsync(fileName);
            if (!exists)
            {
                throw new InvalidOperationException($"File {fileName} was not found after streaming upload");
            }
        }

        var metadata = await _sourceStorageService.GetMetadataAsync(fileName);
        _testOutputHelper.WriteLine($"✅ File {fileName} confirmed to exist with actual size: {metadata.ContentLength:N0} bytes");
    }

    private static string GenerateRandomName(Random random)
    {
        var names = new[]
        {
            "John", "Jane", "Michael", "Sarah", "David", "Lisa", "Robert", "Emma", "William", "Olivia",
            "James", "Sophia", "Benjamin", "Isabella", "Lucas", "Charlotte", "Henry", "Amelia", "Alexander", "Mia"
        };
        return names[random.Next(names.Length)];
    }


    private async Task<string> DownloadAndCalculateMd5Async(BlobStorageService storageService, string fileName)
    {
        _testOutputHelper.WriteLine($"Calculating MD5 hash for file: {fileName}");

        await using var fileStream = await storageService.OpenReadAsync(fileName);
        using var md5 = MD5.Create();

        var hashBytes = await md5.ComputeHashAsync(fileStream);
        var hashString = Convert.ToHexString(hashBytes);

        _testOutputHelper.WriteLine($"MD5 hash for {fileName}: {hashString}");
        return hashString;
    }

    private sealed class TestScope : IAsyncDisposable
    {
        private readonly IBlobStorageService _sourceStorageService;
        private readonly IBlobStorageService _destinationStorageService;
        private readonly string _bucketName;
        private readonly HashSet<string> _keysToCleanup = new();
        private readonly object _lock = new();

        public TestScope(IBlobStorageService sourceStorageService, IBlobStorageService destinationStorageService, string bucketName)
        {
            _sourceStorageService = sourceStorageService ?? throw new ArgumentNullException(nameof(sourceStorageService));
            _destinationStorageService = destinationStorageService ?? throw new ArgumentNullException(nameof(destinationStorageService));
            _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        }

        public void TrackForCleanup(string key)
        {
            if (string.IsNullOrEmpty(key))
                return;

            lock (_lock)
            {
                _keysToCleanup.Add(key);
            }
        }

        public async ValueTask DisposeAsync()
        {
            HashSet<string> keysToClean;
            lock (_lock)
            {
                keysToClean = new HashSet<string>(_keysToCleanup);
                _keysToCleanup.Clear();
            }

            var cleanupTasks = keysToClean.SelectMany(key => new[]
            {
                CleanupFromServiceAsync(_sourceStorageService, key),
                CleanupFromServiceAsync(_destinationStorageService, key)
            });

            await Task.WhenAll(cleanupTasks);
        }

        private async Task CleanupFromServiceAsync(IBlobStorageService service, string key)
        {
            try
            {
                await service.DeleteAsync(key);
            }
            catch
            {
            }
        }
    }
}