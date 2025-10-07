namespace KeeperData.Infrastructure.Storage.Configuration;

public record StorageConfiguration
{
    public StorageWithCredentialsConfiguration ExternalStorage { get; init; } = new();
    public StorageConfigurationDetails InternalStorage { get; init; } = new();

    public required string SourceExternalPrefix { get; init; }
    public required string SourceInternalPrefix { get; init; }
    public required string TargetInternalPrefix { get; init; }
}