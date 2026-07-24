namespace KeeperData.Core.Storage.KeyRotation;

/// <summary>
/// Associated-data purpose strings for <see cref="ISecretProtector"/>, binding each
/// ciphertext to the field it protects.
/// </summary>
public static class SecretPurposes
{
    public const string AccessKeyId = "external-storage:access-key-id";
    public const string SecretAccessKey = "external-storage:secret-access-key";
}
