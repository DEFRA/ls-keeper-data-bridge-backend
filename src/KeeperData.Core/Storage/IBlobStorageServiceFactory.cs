namespace KeeperData.Core.Storage;

public interface IBlobStorageServiceFactory
{
    IBlobStorageService Get();
    IBlobStorageServiceReadOnly GetSource(string type);
    IBlobStorageServiceReadOnly GetSourceExternal();
    IBlobStorageService GetSourceInternal();
}