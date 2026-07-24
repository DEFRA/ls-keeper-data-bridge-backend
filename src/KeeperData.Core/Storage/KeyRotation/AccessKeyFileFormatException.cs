namespace KeeperData.Core.Storage.KeyRotation;

/// <summary>
/// Thrown when an access-key rotation file cannot be parsed.
/// Messages must never contain file contents (the file holds live credentials).
/// </summary>
public class AccessKeyFileFormatException(string message) : Exception(message);
