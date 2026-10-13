using KeeperData.Core.EtlPipeline.Status;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>A value in a normalised file could not be converted to the column type the optimise
/// stage resolved for it - either a type the definition declared, or one auto-detection chose from
/// a sample that did not represent the value.
///
/// Carries the dataset, file, column and record position through to the import status, the way
/// <see cref="SourceFileValidationException"/> does for framing failures: a bare parse error says
/// what failed but not where, and the row number is what lets a reader find it.</summary>
public sealed class SourceFileConversionException(
    string objectKey, string datasetName, string columnName, string targetType, string? value, long recordNumber, Exception? innerException = null)
    : Exception(BuildMessage(objectKey, datasetName, columnName, targetType, recordNumber), innerException), IEtlDiagnosableException
{
    public string ObjectKey { get; } = objectKey;

    public string DatasetName { get; } = datasetName;

    public string ColumnName { get; } = columnName;

    public EtlImportErrorDetail ErrorDetail => new()
    {
        Dataset = DatasetName,
        FileKey = ObjectKey,
        RecordNumber = recordNumber,
        Expected = targetType,
        Actual = value
    };

    private static string BuildMessage(string objectKey, string datasetName, string columnName, string targetType, long recordNumber)
        => $"File '{objectKey}' for dataset '{datasetName}' record {recordNumber}: column '{columnName}' cannot be converted to {targetType}.";
}
