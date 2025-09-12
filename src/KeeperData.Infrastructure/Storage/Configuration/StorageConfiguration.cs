namespace KeeperData.Infrastructure.Storage.Configuration;

public record StorageConfiguration
{
    public StorageWithCredentialsConfiguration ExternalStorage { get; init; } = new();
    public StorageWithCredentialsConfiguration InternalStorage { get; init; } = new();
}