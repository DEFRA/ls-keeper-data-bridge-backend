using KeeperData.Core.EtlPipeline.Status;
using XsvHcdtHelper;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>A source file failed H/C/D/T validation during normalisation.
///
/// The underlying failure is the package's <see cref="XsvValidationException"/>, which says what is
/// wrong with the file but not which file - a reader of the import status needs both. This wrapper
/// supplies the dataset and the object key, and carries the validation rule's expected and actual
/// values through to the status document. The record position joins them once the package reports
/// it.</summary>
public sealed class SourceFileValidationException(
    string objectKey, string datasetName, Exception innerException)
    : Exception(BuildMessage(objectKey, datasetName, innerException), innerException), IEtlDiagnosableException
{
    public string ObjectKey { get; } = objectKey;

    public string DatasetName { get; } = datasetName;

    public EtlImportErrorDetail ErrorDetail => new()
    {
        Dataset = DatasetName,
        FileKey = ObjectKey,
        Expected = (InnerException as XsvValidationException)?.Expected,
        Actual = (InnerException as XsvValidationException)?.Actual
    };

    private static string BuildMessage(string objectKey, string datasetName, Exception innerException)
        => $"File '{objectKey}' for dataset '{datasetName}' failed H/C/D/T validation: {innerException.Message}";
}
