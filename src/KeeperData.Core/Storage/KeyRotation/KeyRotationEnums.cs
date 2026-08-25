namespace KeeperData.Core.Storage.KeyRotation;

/// <summary>
/// How a key rotation record came into being.
/// </summary>
public enum KeyRotationSource
{
    /// <summary>Adopted automatically from a key file found in the external bucket.</summary>
    Automatic,

    /// <summary>Applied manually via the API.</summary>
    Manual,

    /// <summary>Created by rolling back to a previous rotation via the API.</summary>
    Rollback
}

/// <summary>
/// The lifecycle state of a key rotation record.
/// </summary>
public enum KeyRotationStatus
{
    /// <summary>The credentials currently used by the external storage client.</summary>
    Active,

    /// <summary>Previously active credentials, replaced by a newer rotation.</summary>
    Superseded,

    /// <summary>A key file that was detected but failed parsing or validation. Never activated.</summary>
    Failed
}
