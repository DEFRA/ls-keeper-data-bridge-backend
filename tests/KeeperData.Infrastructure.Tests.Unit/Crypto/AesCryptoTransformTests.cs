using FluentAssertions;
using KeeperData.Infrastructure.Crypto;
using System.Text;

namespace KeeperData.Infrastructure.Tests.Unit.Crypto;

public class AesCryptoTransformTests : IDisposable
{
    private const string TestPassword = "2025-08-05_ADDRESSES_CT_01628_DELTA_PROD_UKV_CTSM";
    private const string TestSalt = "Jr8Lm2PXzd7qNbVyWutRfGBxhkHTpE";
    private static readonly byte[] TestSaltBytes = Encoding.UTF8.GetBytes(TestSalt);

    private readonly AesCryptoTransform _cryptoTransform;
    private readonly List<string> _tempFiles = new();
    private readonly string _tempDir;

    public AesCryptoTransformTests()
    {
        _cryptoTransform = new AesCryptoTransform();
        _tempDir = Path.Combine(Path.GetTempPath(), "AesCryptoTransformTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_tempDir);
    }

    [Fact]
    public async Task EncryptFileAsync_WithByteArraySalt_ShouldEncryptFileSuccessfully()
    {
        // Arrange
        var inputFile = CreateTempFile("test input data for encryption");
        var outputFile = GetTempFilePath();
        var progressReports = new List<(int percentage, string status)>();

        // Act
        await _cryptoTransform.EncryptFileAsync(
            inputFile,
            outputFile,
            TestPassword,
            TestSaltBytes,
            (percentage, status) => progressReports.Add((percentage, status)));

        // Assert
        File.Exists(outputFile).Should().BeTrue();
        var encryptedData = await File.ReadAllBytesAsync(outputFile);
        encryptedData.Should().NotBeEmpty();
        encryptedData.Should().NotEqual(Encoding.UTF8.GetBytes("test input data for encryption"));

        progressReports.Should().NotBeEmpty();
        progressReports.Should().Contain(r => r.percentage == 0 && r.status.Contains("Encrypting started"));
        progressReports.Should().Contain(r => r.percentage == 100 && r.status.Contains("Encrypting completed"));
    }

    [Fact]
    public async Task EncryptFileAsync_WithStringSalt_ShouldEncryptFileSuccessfully()
    {
        // Arrange
        var inputFile = CreateTempFile("test input data for encryption");
        var outputFile = GetTempFilePath();

        // Act
        await _cryptoTransform.EncryptFileAsync(inputFile, outputFile, TestPassword, TestSalt);

        // Assert
        File.Exists(outputFile).Should().BeTrue();
        var encryptedData = await File.ReadAllBytesAsync(outputFile);
        encryptedData.Should().NotBeEmpty();
        encryptedData.Should().NotEqual(Encoding.UTF8.GetBytes("test input data for encryption"));
    }

    [Fact]
    public async Task DecryptFileAsync_WithByteArraySalt_ShouldDecryptFileSuccessfully()
    {
        // Arrange
        var originalData = "test data for round-trip encryption/decryption";
        var inputFile = CreateTempFile(originalData);
        var encryptedFile = GetTempFilePath();
        var decryptedFile = GetTempFilePath();
        var progressReports = new List<(int percentage, string status)>();

        // Act
        await _cryptoTransform.EncryptFileAsync(inputFile, encryptedFile, TestPassword, TestSaltBytes);
        await _cryptoTransform.DecryptFileAsync(
            encryptedFile,
            decryptedFile,
            TestPassword,
            TestSaltBytes,
            (percentage, status) => progressReports.Add((percentage, status)));

        // Assert
        File.Exists(decryptedFile).Should().BeTrue();
        var decryptedData = await File.ReadAllTextAsync(decryptedFile);
        decryptedData.Should().Be(originalData);

        progressReports.Should().NotBeEmpty();
        progressReports.Should().Contain(r => r.percentage == 0 && r.status.Contains("Decrypting started"));
        progressReports.Should().Contain(r => r.percentage == 100 && r.status.Contains("Decrypting completed"));
    }

    [Fact]
    public async Task DecryptFileAsync_WithStringSalt_ShouldDecryptFileSuccessfully()
    {
        // Arrange
        var originalData = "test data for round-trip encryption/decryption";
        var inputFile = CreateTempFile(originalData);
        var encryptedFile = GetTempFilePath();
        var decryptedFile = GetTempFilePath();

        // Act
        await _cryptoTransform.EncryptFileAsync(inputFile, encryptedFile, TestPassword, TestSalt);
        await _cryptoTransform.DecryptFileAsync(encryptedFile, decryptedFile, TestPassword, TestSalt);

        // Assert
        File.Exists(decryptedFile).Should().BeTrue();
        var decryptedData = await File.ReadAllTextAsync(decryptedFile);
        decryptedData.Should().Be(originalData);
    }

    [Fact]
    public async Task EncryptFileAsync_NonExistentFile_ShouldThrowFileNotFoundException()
    {
        // Arrange
        var nonExistentFile = Path.Combine(_tempDir, "nonexistent.txt");
        var outputFile = GetTempFilePath();

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(() =>
            _cryptoTransform.EncryptFileAsync(nonExistentFile, outputFile, TestPassword, TestSaltBytes));
    }

    [Fact]
    public async Task DecryptFileAsync_NonExistentFile_ShouldThrowFileNotFoundException()
    {
        // Arrange
        var nonExistentFile = Path.Combine(_tempDir, "nonexistent.txt");
        var outputFile = GetTempFilePath();

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(() =>
            _cryptoTransform.DecryptFileAsync(nonExistentFile, outputFile, TestPassword, TestSaltBytes));
    }

    [Fact]
    public async Task EncryptStreamAsync_WithByteArraySalt_ShouldEncryptStreamSuccessfully()
    {
        // Arrange
        var inputData = "test stream data for encryption";
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(inputData));
        using var outputStream = new MemoryStream();
        var progressReports = new List<(int percentage, string status)>();

        // Act
        await _cryptoTransform.EncryptStreamAsync(
            inputStream,
            outputStream,
            TestPassword,
            TestSaltBytes,
            inputStream.Length,
            (percentage, status) => progressReports.Add((percentage, status)));

        // Assert
        outputStream.Length.Should().BeGreaterThan(0);
        outputStream.ToArray().Should().NotEqual(Encoding.UTF8.GetBytes(inputData));
        progressReports.Should().NotBeEmpty();
    }

    [Fact]
    public async Task EncryptStreamAsync_WithStringSalt_ShouldEncryptStreamSuccessfully()
    {
        // Arrange
        var inputData = "test stream data for encryption";
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(inputData));
        using var outputStream = new MemoryStream();

        // Act
        await _cryptoTransform.EncryptStreamAsync(inputStream, outputStream, TestPassword, TestSalt);

        // Assert
        outputStream.Length.Should().BeGreaterThan(0);
        outputStream.ToArray().Should().NotEqual(Encoding.UTF8.GetBytes(inputData));
    }

    [Fact]
    public async Task DecryptStreamAsync_WithByteArraySalt_ShouldDecryptStreamSuccessfully()
    {
        // Arrange
        var originalData = "test stream data for round-trip encryption/decryption";
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(originalData));
        using var encryptedStream = new MemoryStream();
        using var decryptedStream = new MemoryStream();

        // Act
        await _cryptoTransform.EncryptStreamAsync(inputStream, encryptedStream, TestPassword, TestSaltBytes);
        encryptedStream.Position = 0;
        await _cryptoTransform.DecryptStreamAsync(encryptedStream, decryptedStream, TestPassword, TestSaltBytes);

        // Assert
        var decryptedData = Encoding.UTF8.GetString(decryptedStream.ToArray());
        decryptedData.Should().Be(originalData);
    }

    [Fact]
    public async Task DecryptStreamAsync_WithStringSalt_ShouldDecryptStreamSuccessfully()
    {
        // Arrange
        var originalData = "test stream data for round-trip encryption/decryption";
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(originalData));
        using var encryptedStream = new MemoryStream();
        using var decryptedStream = new MemoryStream();

        // Act
        await _cryptoTransform.EncryptStreamAsync(inputStream, encryptedStream, TestPassword, TestSalt);
        encryptedStream.Position = 0;
        await _cryptoTransform.DecryptStreamAsync(encryptedStream, decryptedStream, TestPassword, TestSalt);

        // Assert
        var decryptedData = Encoding.UTF8.GetString(decryptedStream.ToArray());
        decryptedData.Should().Be(originalData);
    }

    [Fact]
    public async Task EncryptDecrypt_WithEmptySalt_ShouldWorkCorrectly()
    {
        // Arrange
        var originalData = "test data with empty salt";
        var emptySalt = Array.Empty<byte>();
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(originalData));
        using var encryptedStream = new MemoryStream();
        using var decryptedStream = new MemoryStream();

        // Act
        await _cryptoTransform.EncryptStreamAsync(inputStream, encryptedStream, TestPassword, emptySalt);
        encryptedStream.Position = 0;
        await _cryptoTransform.DecryptStreamAsync(encryptedStream, decryptedStream, TestPassword, emptySalt);

        // Assert
        var decryptedData = Encoding.UTF8.GetString(decryptedStream.ToArray());
        decryptedData.Should().Be(originalData);
    }

    [Fact]
    public async Task EncryptDecrypt_WithShortSalt_ShouldWorkCorrectly()
    {
        // Arrange
        var originalData = "test data with short salt";
        var shortSalt = new byte[] { 1, 2, 3 }; // Less than 8 bytes
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(originalData));
        using var encryptedStream = new MemoryStream();
        using var decryptedStream = new MemoryStream();

        // Act
        await _cryptoTransform.EncryptStreamAsync(inputStream, encryptedStream, TestPassword, shortSalt);
        encryptedStream.Position = 0;
        await _cryptoTransform.DecryptStreamAsync(encryptedStream, decryptedStream, TestPassword, shortSalt);

        // Assert
        var decryptedData = Encoding.UTF8.GetString(decryptedStream.ToArray());
        decryptedData.Should().Be(originalData);
    }

    [Fact]
    public async Task EncryptDecrypt_WithNullEmptyStringSalt_ShouldWorkCorrectly()
    {
        // Arrange
        var originalData = "test data with null/empty string salt";
        using var inputStream1 = new MemoryStream(Encoding.UTF8.GetBytes(originalData));
        using var encryptedStream1 = new MemoryStream();
        using var decryptedStream1 = new MemoryStream();
        using var inputStream2 = new MemoryStream(Encoding.UTF8.GetBytes(originalData));
        using var encryptedStream2 = new MemoryStream();
        using var decryptedStream2 = new MemoryStream();

        // Act - Test with null string salt
        await _cryptoTransform.EncryptStreamAsync(inputStream1, encryptedStream1, TestPassword, (string)null!);
        encryptedStream1.Position = 0;
        await _cryptoTransform.DecryptStreamAsync(encryptedStream1, decryptedStream1, TestPassword, (string)null!);

        // Act - Test with empty string salt
        await _cryptoTransform.EncryptStreamAsync(inputStream2, encryptedStream2, TestPassword, "");
        encryptedStream2.Position = 0;
        await _cryptoTransform.DecryptStreamAsync(encryptedStream2, decryptedStream2, TestPassword, "");

        // Assert
        var decryptedData1 = Encoding.UTF8.GetString(decryptedStream1.ToArray());
        var decryptedData2 = Encoding.UTF8.GetString(decryptedStream2.ToArray());
        decryptedData1.Should().Be(originalData);
        decryptedData2.Should().Be(originalData);
    }

    [Fact]
    public async Task EncryptStreamAsync_WithCancellation_ShouldThrowOperationCanceledException()
    {
        // Arrange
        var inputData = new byte[1024 * 1024]; // 1MB of data
        new Random().NextBytes(inputData);
        using var inputStream = new MemoryStream(inputData);
        using var outputStream = new MemoryStream();
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            _cryptoTransform.EncryptStreamAsync(inputStream, outputStream, TestPassword, TestSaltBytes,
                cancellationToken: cts.Token));

        exception.Should().NotBeNull();
    }

    [Fact]
    public async Task DecryptStreamAsync_WithCancellation_ShouldThrowOperationCanceledException()
    {
        // Arrange
        var inputData = new byte[1024 * 1024]; // 1MB of data
        new Random().NextBytes(inputData);
        using var inputStream = new MemoryStream(inputData);
        using var outputStream = new MemoryStream();
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            _cryptoTransform.DecryptStreamAsync(inputStream, outputStream, TestPassword, TestSaltBytes,
                cancellationToken: cts.Token));

        exception.Should().NotBeNull();
    }

    [Fact]
    public async Task ProgressCallback_ShouldReportCorrectProgress()
    {
        // Arrange
        var inputData = new byte[1024 * 100]; // 100KB to ensure multiple progress reports
        new Random().NextBytes(inputData);
        var inputFile = CreateTempFile(inputData);
        var outputFile = GetTempFilePath();
        var progressReports = new List<(int percentage, string status)>();

        // Act
        await _cryptoTransform.EncryptFileAsync(
            inputFile,
            outputFile,
            TestPassword,
            TestSaltBytes,
            (percentage, status) => progressReports.Add((percentage, status)));

        // Assert
        progressReports.Should().NotBeEmpty();
        progressReports.Should().Contain(r => r.percentage == 0);
        progressReports.Should().Contain(r => r.percentage == 100);
        progressReports.Should().OnlyContain(r => r.percentage >= 0 && r.percentage <= 100);
        progressReports.Select(r => r.status).Should().OnlyContain(s => s.Contains("Encrypting"));
    }

    [Fact]
    public async Task StreamProcessing_WithoutTotalBytes_ShouldReportProgressWithoutPercentage()
    {
        // Arrange
        var inputData = "test data without total bytes";
        using var inputStream = new MemoryStream(Encoding.UTF8.GetBytes(inputData));
        using var outputStream = new MemoryStream();
        var progressReports = new List<(int percentage, string status)>();

        // Act
        await _cryptoTransform.EncryptStreamAsync(
            inputStream,
            outputStream,
            TestPassword,
            TestSaltBytes,
            totalBytes: null, // No total bytes provided
            (percentage, status) => progressReports.Add((percentage, status)));

        // Assert
        progressReports.Should().NotBeEmpty();
        progressReports.Should().Contain(r => r.percentage == 0 && r.status.Contains("Encrypting started"));
        progressReports.Should().Contain(r => r.percentage == 100 && r.status.Contains("Encrypting completed"));
        progressReports.Should().Contain(r => r.percentage == 0 && r.status.Contains("processed") && !r.status.Contains("%"));
    }

    [Fact]
    public async Task LargeFileStreaming_Over500MB_ShouldProcessWithMinimalMemoryUsage()
    {
        // Arrange
        const long fileSizeBytes = 500L * 1024 * 1024 + 1024; // 500MB + 1KB
        var inputFile = CreateLargeTempFile(fileSizeBytes);
        var encryptedFile = GetTempFilePath();
        var decryptedFile = GetTempFilePath();

        var encryptProgressReports = new List<(int percentage, string status)>();
        var decryptProgressReports = new List<(int percentage, string status)>();

        // Monitor memory usage
        var initialMemory = GC.GetTotalMemory(true);

        // Act - Encrypt
        await _cryptoTransform.EncryptFileAsync(
            inputFile,
            encryptedFile,
            TestPassword,
            TestSaltBytes,
            (percentage, status) => encryptProgressReports.Add((percentage, status)));

        var memoryAfterEncrypt = GC.GetTotalMemory(false);

        // Act - Decrypt
        await _cryptoTransform.DecryptFileAsync(
            encryptedFile,
            decryptedFile,
            TestPassword,
            TestSaltBytes,
            (percentage, status) => decryptProgressReports.Add((percentage, status)));

        var memoryAfterDecrypt = GC.GetTotalMemory(false);

        // Assert file sizes
        var originalFileInfo = new FileInfo(inputFile);
        var decryptedFileInfo = new FileInfo(decryptedFile);

        originalFileInfo.Length.Should().Be(fileSizeBytes);
        decryptedFileInfo.Length.Should().Be(fileSizeBytes);

        // Assert memory usage - should not buffer entire file in memory
        // Memory increase should be much less than file size (allowing some overhead for test framework)
        var maxMemoryIncrease = Math.Max(memoryAfterEncrypt - initialMemory, memoryAfterDecrypt - initialMemory);
        maxMemoryIncrease.Should().BeLessThan(10 * 1024 * 1024); // Less than 10MB increase

        // Assert progress reporting
        encryptProgressReports.Should().NotBeEmpty();
        encryptProgressReports.Should().Contain(r => r.percentage == 100);
        encryptProgressReports.Where(r => r.status.Contains("%")).Should().NotBeEmpty();

        decryptProgressReports.Should().NotBeEmpty();
        decryptProgressReports.Should().Contain(r => r.percentage == 100);
        decryptProgressReports.Where(r => r.status.Contains("%")).Should().NotBeEmpty();

        // Verify data integrity by comparing checksums of first and last chunks
        await VerifyFileIntegrity(inputFile, decryptedFile);
    }

    [Fact]
    public async Task StreamDecryption_LargeFile_ShouldMaintainConstantMemoryUsage()
    {
        // Arrange
        const long fileSizeBytes = 100L * 1024 * 1024; // 100MB for faster test execution
        var testData = GenerateTestData(1024); // 1KB pattern
        var inputFile = CreateLargeTempFileWithPattern(fileSizeBytes, testData);
        var encryptedFile = GetTempFilePath();
        var decryptedFile = GetTempFilePath();

        // Act
        await _cryptoTransform.EncryptFileAsync(inputFile, encryptedFile, TestPassword, TestSaltBytes);

        // Monitor memory during streaming decryption
        var memoryBefore = GC.GetTotalMemory(true);

        await _cryptoTransform.DecryptFileAsync(encryptedFile, decryptedFile, TestPassword, TestSaltBytes);

        var memoryAfter = GC.GetTotalMemory(false);

        // Assert
        // Verify the decrypted file matches original
        await VerifyFileIntegrity(inputFile, decryptedFile);

        // Memory usage should remain constant (not proportional to file size)
        var memoryIncrease = memoryAfter - memoryBefore;
        memoryIncrease.Should().BeLessThan(5 * 1024 * 1024); // Less than 5MB increase
    }

    private string CreateTempFile(string content)
    {
        var filePath = GetTempFilePath();
        File.WriteAllText(filePath, content);
        return filePath;
    }

    private string CreateTempFile(byte[] content)
    {
        var filePath = GetTempFilePath();
        File.WriteAllBytes(filePath, content);
        return filePath;
    }

    private string CreateLargeTempFile(long sizeBytes)
    {
        var filePath = GetTempFilePath();
        using var stream = new FileStream(filePath, FileMode.Create, FileAccess.Write);

        var buffer = new byte[64 * 1024]; // 64KB buffer
        new Random(42).NextBytes(buffer); // Use seed for reproducible data

        long written = 0;
        while (written < sizeBytes)
        {
            var toWrite = (int)Math.Min(buffer.Length, sizeBytes - written);
            stream.Write(buffer, 0, toWrite);
            written += toWrite;
        }

        return filePath;
    }

    private string CreateLargeTempFileWithPattern(long sizeBytes, byte[] pattern)
    {
        var filePath = GetTempFilePath();
        using var stream = new FileStream(filePath, FileMode.Create, FileAccess.Write);

        long written = 0;
        while (written < sizeBytes)
        {
            var toWrite = (int)Math.Min(pattern.Length, sizeBytes - written);
            stream.Write(pattern, 0, toWrite);
            written += toWrite;
        }

        return filePath;
    }

    private static byte[] GenerateTestData(int size)
    {
        var data = new byte[size];
        for (int i = 0; i < size; i++)
        {
            data[i] = (byte)(i % 256);
        }
        return data;
    }

    private async Task VerifyFileIntegrity(string originalFile, string decryptedFile)
    {
        using var originalStream = new FileStream(originalFile, FileMode.Open, FileAccess.Read);
        using var decryptedStream = new FileStream(decryptedFile, FileMode.Open, FileAccess.Read);

        // Compare file sizes
        originalStream.Length.Should().Be(decryptedStream.Length);

        // Compare first 4KB
        var originalBuffer = new byte[4096];
        var decryptedBuffer = new byte[4096];

        var originalRead = await originalStream.ReadAsync(originalBuffer);
        var decryptedRead = await decryptedStream.ReadAsync(decryptedBuffer);

        originalRead.Should().Be(decryptedRead);
        originalBuffer.AsSpan(0, originalRead).ToArray()
            .Should().Equal(decryptedBuffer.AsSpan(0, decryptedRead).ToArray());

        // Compare last 4KB
        if (originalStream.Length > 4096)
        {
            originalStream.Seek(-4096, SeekOrigin.End);
            decryptedStream.Seek(-4096, SeekOrigin.End);

            originalRead = await originalStream.ReadAsync(originalBuffer);
            decryptedRead = await decryptedStream.ReadAsync(decryptedBuffer);

            originalRead.Should().Be(decryptedRead);
            originalBuffer.AsSpan(0, originalRead).ToArray()
                .Should().Equal(decryptedBuffer.AsSpan(0, decryptedRead).ToArray());
        }
    }

    private string GetTempFilePath()
    {
        var filePath = Path.Combine(_tempDir, $"test_{Guid.NewGuid()}.tmp");
        _tempFiles.Add(filePath);
        return filePath;
    }

    public void Dispose()
    {
        foreach (var file in _tempFiles.Where(File.Exists))
        {
            try
            {
                File.Delete(file);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        if (Directory.Exists(_tempDir))
        {
            try
            {
                Directory.Delete(_tempDir, true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}