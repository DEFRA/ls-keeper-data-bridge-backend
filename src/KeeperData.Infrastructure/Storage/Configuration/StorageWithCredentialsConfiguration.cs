namespace KeeperData.Infrastructure.Storage.Configuration;

public record StorageWithCredentialsConfiguration : StorageConfigurationDetails
{
    public string AccessKeySecretName { get; init; } = string.Empty;
    public string SecretKeySecretName { get; init; } = string.Empty;
}