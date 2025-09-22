using KeeperData.Core.Crypto;
using System.Security.Cryptography;
using System.Text;

namespace KeeperData.Infrastructure.Crypto;

public class AesCryptoTransform : IAesCryptoTransform
{
    private const int PbeKeySpecIterationsDefault = 32;
    private const int PbeKeySpecKeyLenDefault = 256;
    private const int BufferSize = 64 * 1024;
    private const int ProgressReportInterval = 1;

    public async Task EncryptFileAsync(string inputFilePath, string outputFilePath, string password, byte[] salt,
        ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default)
    {
        if (!File.Exists(inputFilePath))
        {
            throw new FileNotFoundException($"Input file not found: {inputFilePath}");
        }

        var fileInfo = new FileInfo(inputFilePath);
        var totalBytes = fileInfo.Length;

        using var inputStream = new FileStream(inputFilePath, FileMode.Open, FileAccess.Read);
        using var outputStream = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write);

        await EncryptStreamAsync(inputStream, outputStream, password, salt, totalBytes, progressCallback, cancellationToken);
    }

    public async Task DecryptFileAsync(string inputFilePath,
                                       string outputFilePath,
                                       string password,
                                       byte[] salt,
                                       ProgressCallback? progressCallback = null,
                                       CancellationToken cancellationToken = default)
    {
        if (!File.Exists(inputFilePath))
        {
            throw new FileNotFoundException($"Input file not found: {inputFilePath}");
        }

        var fileInfo = new FileInfo(inputFilePath);
        var totalBytes = fileInfo.Length;

        using var inputStream = new FileStream(inputFilePath, FileMode.Open, FileAccess.Read);
        using var outputStream = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write);

        await DecryptStreamAsync(inputStream, outputStream, password, salt, totalBytes, progressCallback, cancellationToken);
    }

    public async Task EncryptStreamAsync(Stream inputStream,
                                         Stream outputStream,
                                         string password,
                                         byte[] salt,
                                         long? totalBytes = null,
                                         ProgressCallback? progressCallback = null,
                                         CancellationToken cancellationToken = default)
    {
        var key = DeriveKey(password, salt);

        using var aes = Aes.Create();
        aes.Key = key;
        aes.Mode = CipherMode.ECB;
        aes.Padding = PaddingMode.PKCS7;

        using var encryptor = aes.CreateEncryptor();
        using var cryptoStream = new CryptoStream(outputStream, encryptor, CryptoStreamMode.Write, leaveOpen: true);

        await ProcessStreamAsync(inputStream, cryptoStream, totalBytes, progressCallback, "Encrypting", cancellationToken);

        cryptoStream.FlushFinalBlock();
    }

    public async Task DecryptStreamAsync(Stream inputStream,
                                         Stream outputStream,
                                         string password,
                                         byte[] salt,
                                         long? totalBytes = null,
                                         ProgressCallback? progressCallback = null,
                                         CancellationToken cancellationToken = default)
    {
        var key = DeriveKey(password, salt);

        using var aes = Aes.Create();
        aes.Key = key;
        aes.Mode = CipherMode.ECB;
        aes.Padding = PaddingMode.PKCS7;

        using var decryptor = aes.CreateDecryptor();
        using var cryptoStream = new CryptoStream(inputStream, decryptor, CryptoStreamMode.Read, leaveOpen: true);

        await ProcessStreamAsync(cryptoStream, outputStream, totalBytes, progressCallback, "Decrypting", cancellationToken);
    }

    public async Task EncryptFileAsync(string inputFilePath,
                                       string outputFilePath,
                                       string password,
                                       string salt,
                                       ProgressCallback? progressCallback = null,
                                       CancellationToken cancellationToken = default)
    {
        var saltBytes = string.IsNullOrEmpty(salt) ? new byte[0] : Encoding.UTF8.GetBytes(salt);
        await EncryptFileAsync(inputFilePath, outputFilePath, password, saltBytes, progressCallback, cancellationToken);
    }

    public async Task DecryptFileAsync(string inputFilePath,
                                       string outputFilePath,
                                       string password,
                                       string salt,
                                       ProgressCallback? progressCallback = null,
                                       CancellationToken cancellationToken = default)
    {
        var saltBytes = string.IsNullOrEmpty(salt) ? new byte[0] : Encoding.UTF8.GetBytes(salt);
        await DecryptFileAsync(inputFilePath, outputFilePath, password, saltBytes, progressCallback, cancellationToken);
    }

    public async Task EncryptStreamAsync(Stream inputStream,
                                         Stream outputStream,
                                         string password,
                                         string salt,
                                         long? totalBytes = null,
                                         ProgressCallback? progressCallback = null,
                                         CancellationToken cancellationToken = default)
    {
        var saltBytes = string.IsNullOrEmpty(salt) ? new byte[0] : Encoding.UTF8.GetBytes(salt);
        await EncryptStreamAsync(inputStream, outputStream, password, saltBytes, totalBytes, progressCallback, cancellationToken);
    }

    public async Task DecryptStreamAsync(Stream inputStream,
                                         Stream outputStream,
                                         string password,
                                         string salt,
                                         long? totalBytes = null,
                                         ProgressCallback? progressCallback = null,
                                         CancellationToken cancellationToken = default)
    {
        var saltBytes = string.IsNullOrEmpty(salt) ? new byte[0] : Encoding.UTF8.GetBytes(salt);
        await DecryptStreamAsync(inputStream, outputStream, password, saltBytes, totalBytes, progressCallback, cancellationToken);
    }

    private static byte[] DeriveKey(string password, byte[] salt)
    {
        var actualSalt = salt;
        if (salt.Length == 0)
        {
            actualSalt = new byte[8];
        }
        else if (salt.Length < 8)
        {
            actualSalt = new byte[8];
            Array.Copy(salt, actualSalt, salt.Length);
        }

        using var pbkdf2 = new Rfc2898DeriveBytes(password, actualSalt, PbeKeySpecIterationsDefault, HashAlgorithmName.SHA1);
        return pbkdf2.GetBytes(PbeKeySpecKeyLenDefault / 8);
    }

    private static async Task ProcessStreamAsync(Stream inputStream,
                                                 Stream outputStream,
                                                 long? totalBytes,
                                                 ProgressCallback? progressCallback,
                                                 string operation,
                                                 CancellationToken cancellationToken = default)
    {
        var buffer = new byte[BufferSize];
        long totalBytesProcessed = 0;
        var lastReportedProgress = -1;

        progressCallback?.Invoke(0, $"{operation} started");

        int bytesRead;
        while ((bytesRead = await inputStream.ReadAsync(buffer, cancellationToken)) > 0)
        {
            await outputStream.WriteAsync(buffer.AsMemory(0, bytesRead), cancellationToken);
            totalBytesProcessed += bytesRead;

            if (totalBytes.HasValue && totalBytes.Value > 0 && progressCallback != null)
            {
                var progressPercentage = (int)(totalBytesProcessed * 100 / totalBytes.Value);

                if (progressPercentage != lastReportedProgress && progressPercentage % ProgressReportInterval == 0)
                {
                    progressCallback.Invoke(progressPercentage,
                        $"{operation} {progressPercentage}% - {FormatBytes(totalBytesProcessed)} of {FormatBytes(totalBytes.Value)}");
                    lastReportedProgress = progressPercentage;
                }
            }
            else if (progressCallback != null)
            {
                progressCallback.Invoke(0, $"{operation} - {FormatBytes(totalBytesProcessed)} processed");
            }
        }

        if (outputStream is not CryptoStream)
        {
            await outputStream.FlushAsync(cancellationToken);
        }

        progressCallback?.Invoke(100, $"{operation} completed - {FormatBytes(totalBytesProcessed)} processed");
    }

    private static string FormatBytes(long bytes)
    {
        const long kb = 1024;
        const long mb = kb * 1024;
        const long gb = mb * 1024;

        return bytes switch
        {
            >= gb => $"{bytes / (double)gb:F2} GB",
            >= mb => $"{bytes / (double)mb:F2} MB",
            >= kb => $"{bytes / (double)kb:F2} KB",
            _ => $"{bytes} bytes"
        };
    }
}