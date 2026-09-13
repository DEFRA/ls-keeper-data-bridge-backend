namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>A source file failed H/C/D/T validation during normalisation.
///
/// The underlying failure is the package's <see cref="XsvHcdtHelper.XsvValidationException"/>, which
/// says what is wrong with the file but not which file or which record - both of which a reader of
/// the import status needs. This wrapper supplies them. The record's content is deliberately absent
/// from the message, which is served to API callers; the log carries the record's masked shape.</summary>
public sealed class SourceFileValidationException(
    string objectKey, string datasetName, int? recordNumber, Exception innerException)
    : Exception(BuildMessage(objectKey, datasetName, recordNumber, innerException), innerException), IEtlDiagnosableException
{
    public string ObjectKey { get; } = objectKey;

    public string DatasetName { get; } = datasetName;

    /// <summary>The 1-based position of the first record the strict field check rejected, counting
    /// every record in the file, or null when the failure was not a record-level one (for example a
    /// trailer count mismatch).</summary>
    public int? RecordNumber { get; } = recordNumber;

    private static string BuildMessage(string objectKey, string datasetName, int? recordNumber, Exception innerException)
        => recordNumber is { } record
            ? $"File '{objectKey}' for dataset '{datasetName}' failed H/C/D/T validation at record {record}: {innerException.Message}"
            : $"File '{objectKey}' for dataset '{datasetName}' failed H/C/D/T validation: {innerException.Message}";
}
