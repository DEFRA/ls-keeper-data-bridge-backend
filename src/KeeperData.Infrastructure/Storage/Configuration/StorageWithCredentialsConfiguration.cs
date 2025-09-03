namespace KeeperData.Infrastructure.Storage.Configuration;

public record StorageWithCredentialsConfiguration
{
    public bool HealthcheckEnabled { get; init; }
    public string BucketName { get; init; } = string.Empty;
    public string AccessKeySecretName { get; init; } = string.Empty;
    public string SecretKeySecretName { get; init; } = string.Empty;
}
