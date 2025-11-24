namespace KeeperData.Core.Reporting;

/// <summary>
/// Represents the processing status of a file during import.
/// </summary>
public enum FileProcessingStatus
{
    /// <summary>
    /// The file has been discovered but not yet acquired.
    /// </summary>
    Discovered,

    /// <summary>
    /// The file has been successfully acquired and decrypted.
    /// </summary>
    Acquired,

    /// <summary>
    /// The file has been successfully ingested and its data processed.
    /// </summary>
    Ingested,

    /// <summary>
    /// Processing of the file failed.
    /// </summary>
    Failed
}