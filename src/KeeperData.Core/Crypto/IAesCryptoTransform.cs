namespace KeeperData.Core.Crypto;

public delegate void ProgressCallback(int progressPercentage, string status);

public interface IAesCryptoTransform
{
    Task EncryptFileAsync(string inputFilePath, string outputFilePath, string password, byte[] salt,
        ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default);

    Task EncryptFileAsync(string inputFilePath, string outputFilePath, string password, string salt,
        ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default);

    Task DecryptFileAsync(string inputFilePath, string outputFilePath, string password, byte[] salt,
        ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default);

    Task DecryptFileAsync(string inputFilePath, string outputFilePath, string password, string salt,
        ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default);

    Task EncryptStreamAsync(Stream inputStream, Stream outputStream, string password, byte[] salt,
        long? totalBytes = null, ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default);

    Task EncryptStreamAsync(Stream inputStream, Stream outputStream, string password, string salt,
        long? totalBytes = null, ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default);

    Task DecryptStreamAsync(Stream inputStream, Stream outputStream, string password, byte[] salt,
        long? totalBytes = null, ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default);

    Task DecryptStreamAsync(Stream inputStream, Stream outputStream, string password, string salt,
        long? totalBytes = null, ProgressCallback? progressCallback = null, CancellationToken cancellationToken = default);
}