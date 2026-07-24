namespace KeeperData.Core.Storage.KeyRotation;

/// <summary>
/// Derives the expected access-key rotation file name from an external bucket name.
/// E.g. <c>cerespfm-prd-prd1-livestockfeeds</c> → specifier <c>prd1</c> → <c>Prd1_LI_CDP_Int_User_accessKeys.csv</c>.
/// </summary>
public static class KeyRotationFileNameResolver
{
    public const string FileNameSuffix = "_LI_CDP_Int_User_accessKeys.csv";

    /// <summary>
    /// Resolves the rotation file name for the given bucket by taking the third
    /// hyphen-separated segment of the bucket name and upper-casing its first letter.
    /// </summary>
    /// <exception cref="InvalidOperationException">The bucket name has no usable third segment.</exception>
    public static string Resolve(string bucketName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bucketName);

        var segments = bucketName.Split('-');
        if (segments.Length < 3 || string.IsNullOrWhiteSpace(segments[2]))
        {
            throw new InvalidOperationException(
                $"Cannot derive the key rotation file name: bucket name '{bucketName}' has no specifier as its third hyphen-separated segment.");
        }

        var specifier = segments[2];
        return $"{char.ToUpperInvariant(specifier[0])}{specifier[1..]}{FileNameSuffix}";
    }
}
