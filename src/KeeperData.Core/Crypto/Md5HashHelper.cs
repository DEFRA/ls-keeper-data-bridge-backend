using System.Security.Cryptography;

namespace KeeperData.Core.Crypto;

/// <summary>
/// Helper class for calculating MD5 hashes of stream content.
/// </summary>
public static class Md5HashHelper
{
    /// <summary>
    /// Calculates MD5 hash of a stream without consuming it.
    /// The stream position is reset to the beginning after calculation.
    /// </summary>
    /// <param name="stream">The stream to calculate hash for. Must be seekable.</param>
    /// <returns>MD5 hash as hexadecimal string.</returns>
    public static async Task<string> CalculateMd5Async(Stream stream, CancellationToken ct = default)
    {
        if (!stream.CanSeek)
        {
            throw new ArgumentException("Stream must be seekable to calculate MD5 hash", nameof(stream));
        }

        var originalPosition = stream.Position;
        
        try
        {
            stream.Position = 0;
            var hash = await MD5.HashDataAsync(stream, ct);
            return Convert.ToHexString(hash).ToLowerInvariant();
        }
        finally
        {
            stream.Position = originalPosition;
        }
    }
    
    /// <summary>
    /// Calculates MD5 hash while copying data from source to destination stream.
    /// This is useful for calculating hash during file transfer operations.
    /// </summary>
    public static async Task<string> CalculateMd5WhileCopyingAsync(
        Stream source, 
        Stream destination, 
        CancellationToken ct = default)
    {
        using var md5 = MD5.Create();
        using var cryptoStream = new CryptoStream(destination, md5, CryptoStreamMode.Write, leaveOpen: true);
        
        await source.CopyToAsync(cryptoStream, ct);
        await cryptoStream.FlushFinalBlockAsync(ct);
        
        return Convert.ToHexString(md5.Hash ?? Array.Empty<byte>()).ToLowerInvariant();
    }
}
