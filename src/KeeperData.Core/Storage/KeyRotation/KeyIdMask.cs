namespace KeeperData.Core.Storage.KeyRotation;

/// <summary>
/// Produces display-safe masked access key ids: the first three and last three
/// characters with an ellipsis between (e.g. <c>AKI...XYZ</c>).
/// </summary>
public static class KeyIdMask
{
    /// <summary>
    /// Masks the key id. Ids of six characters or fewer are fully masked so that
    /// the hint never reveals the entire value.
    /// </summary>
    public static string Mask(string? keyId)
    {
        if (string.IsNullOrEmpty(keyId))
        {
            return string.Empty;
        }

        return keyId.Length <= 6
            ? new string('*', keyId.Length)
            : $"{keyId[..3]}...{keyId[^3..]}";
    }
}
