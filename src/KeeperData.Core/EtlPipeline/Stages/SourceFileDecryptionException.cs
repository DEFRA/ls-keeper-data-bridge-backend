namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>A source file could not be decrypted.
///
/// The underlying failure is a padding error from AES, which says nothing a reader can act on. There
/// are only two causes: the file was encrypted against a different salt from the one this
/// environment is configured with, or the filename has changed since it was encrypted — the object
/// key is the decryption password, so renaming a file makes it undecryptable.
///
/// The salt is deliberately absent from the message, which is served to API callers.</summary>
public sealed class SourceFileDecryptionException(string objectKey, string datasetName, Exception innerException)
    : Exception(BuildMessage(objectKey, datasetName), innerException), IEtlDiagnosableException
{
    public string ObjectKey { get; } = objectKey;

    public string DatasetName { get; } = datasetName;

    private static string BuildMessage(string objectKey, string datasetName)
        => $"Could not decrypt '{objectKey}' for dataset '{datasetName}'. " +
           "The file must be encrypted with this environment's configured salt, and must still have " +
           "the filename it was encrypted with, because the filename is the decryption password.";
}
