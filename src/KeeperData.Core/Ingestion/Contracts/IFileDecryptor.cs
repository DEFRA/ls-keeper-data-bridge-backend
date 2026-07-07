namespace KeeperData.Core.Ingestion.Contracts;

/// <summary>Streams ciphertext -> plaintext (wraps the existing AesCryptoTransform + salt).</summary>
public interface IFileDecryptor
{
    Task DecryptAsync(Stream input, Stream output, string password, CancellationToken cancellationToken);
}
