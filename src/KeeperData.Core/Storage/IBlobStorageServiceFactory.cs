namespace KeeperData.Core.Storage;

public interface IBlobStorageServiceFactory
{
    /// <summary>
    /// Gets the main blob service
    /// </summary>
    /// <returns></returns>
    /// <remarks>For the _main_ blob service, i.e., the target or the destination</remarks>
    IBlobStorageService Get();
    IBlobStorageService GetCleanseReportsBlobService();
    IBlobStorageServiceReadOnly GetSource(string type);
    IBlobStorageServiceReadOnly GetSourceExternal();
    IBlobStorageService GetSourceInternal();
}