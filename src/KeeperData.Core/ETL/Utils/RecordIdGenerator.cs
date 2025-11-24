using System.Security.Cryptography;
using System.Text;

namespace KeeperData.Core.ETL.Utils;

/// <summary>
/// Generates URL-safe record identifiers from composite key parts using SHA256 hashing.
/// Encapsulates all concerns around key generation, ensuring consistent and collision-resistant IDs.
/// </summary>
public class RecordIdGenerator
{
    /// <summary>
    /// Generates a URL-safe record ID from one or more key parts.
    /// Uses SHA256 hashing to create a consistent, collision-resistant identifier.
    /// The hash is Base64URL-encoded for URL safety (43 characters).
    /// </summary>
    /// <param name="keyParts">The individual parts of the composite key</param>
    /// <returns>A URL-safe record ID string (43 characters for SHA256)</returns>
    /// <exception cref="ArgumentNullException">Thrown when keyParts is null</exception>
    /// <exception cref="ArgumentException">Thrown when keyParts is empty or contains null/empty values</exception>
    public string GenerateId(params string[] keyParts)
    {
        if (keyParts == null)
        {
            throw new ArgumentNullException(nameof(keyParts));
        }

        if (keyParts.Length == 0)
        {
            throw new ArgumentException("At least one key part is required", nameof(keyParts));
        }

        // Validate all parts are non-null and non-empty
        for (int i = 0; i < keyParts.Length; i++)
        {
            if (string.IsNullOrEmpty(keyParts[i]))
            {
                throw new ArgumentException($"Key part at index {i} is null or empty", nameof(keyParts));
            }
        }

        // Join parts with delimiter and hash
        var composite = string.Join(EtlConstants.CompositeKeyDelimiter, keyParts);
        return HashToBase64Url(composite);
    }

    /// <summary>
    /// Generates a URL-safe record ID from an enumerable of key parts.
    /// Uses SHA256 hashing to create a consistent, collision-resistant identifier.
    /// </summary>
    /// <param name="keyParts">The individual parts of the composite key</param>
    /// <returns>A URL-safe record ID string</returns>
    /// <exception cref="ArgumentNullException">Thrown when keyParts is null</exception>
    /// <exception cref="ArgumentException">Thrown when keyParts is empty or contains null/empty values</exception>
    public string GenerateId(IEnumerable<string> keyParts)
    {
        if (keyParts == null)
        {
            throw new ArgumentNullException(nameof(keyParts));
        }

        return GenerateId(keyParts.ToArray());
    }

    /// <summary>
    /// Computes a SHA256 hash of the input string and returns it as a Base64URL-encoded string.
    /// Base64URL encoding uses '-' instead of '+', '_' instead of '/', and removes padding '='.
    /// </summary>
    /// <param name="value">The string to hash</param>
    /// <returns>Base64URL-encoded SHA256 hash (43 characters)</returns>
    private static string HashToBase64Url(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var hash = SHA256.HashData(bytes);

        // Convert to Base64URL format
        var base64 = Convert.ToBase64String(hash);
        return base64
            .Replace('+', '-')  // Replace + with -
            .Replace('/', '_')  // Replace / with _
            .TrimEnd('=');      // Remove padding
    }
}