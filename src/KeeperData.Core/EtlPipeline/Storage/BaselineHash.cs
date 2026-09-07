using System.Security.Cryptography;
using System.Text;

namespace KeeperData.Core.EtlPipeline.Storage;

/// <summary>
/// Identifies the set of baseline (bulk) files a snapshot was built from. Bulk files are a set, not a
/// sequence: their names are sorted ordinally and joined with a newline, so the hash is independent of
/// the order they were discovered in.
///
/// A name carrying the delimiter is rejected rather than joined, because two names and one name holding
/// a newline between them would otherwise hash alike, and the hash is what decides whether the dataset
/// rebuilds. No object in the feed is named that way; the guard is there so the ambiguity cannot arise
/// silently if one ever is.
///
/// A dataset with no baseline lane has an empty set and therefore one constant hash forever, which is
/// what keeps the reset protocol inert for the datasets that have no bulk files.
/// </summary>
public static class BaselineHash
{
    /// <summary>Hex characters in a hash. Four bytes of SHA-256: enough that two bulk sets colliding is
    /// not a practical concern, short enough to sit in a file name.</summary>
    public const int Length = 8;

    public static string Compute(IEnumerable<string> bulkKeys)
    {
        ArgumentNullException.ThrowIfNull(bulkKeys);

        var keys = bulkKeys.OrderBy(key => key, StringComparer.Ordinal).ToList();

        // string.Contains has an overload that accepts a StringComparison; pass a string, not a char.
        // Detect any key that contains the newline separator and report the first offending key
        var bad = keys.FirstOrDefault(k => k.Contains("\n", StringComparison.Ordinal));
        if (bad is not null)
        {
            throw new ArgumentException(
                $"Baseline file name '{bad}' carries a newline, which the hash uses to separate names",
                nameof(bulkKeys));
        }

        var joined = string.Join('\n', keys);
        var digest = SHA256.HashData(Encoding.UTF8.GetBytes(joined));

        return Convert.ToHexString(digest.AsSpan(0, Length / 2)).ToLowerInvariant();
    }
}
