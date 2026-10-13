using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.EtlPipeline.Status;

/// <summary>Structured context about a failed import, persisted on the import document and served
/// to callers alongside the readable <c>Error</c> summary.
///
/// Every member is optional: a failure deep in the pipeline can name the dataset, file and record
/// it choked on, while a failure outside one can still say which stage was running and what type of
/// exception it was. Fields are filled from what the failing code actually knew - never guessed.
/// Like <c>Error</c>, nothing here may carry a salt, password or presigned URL.</summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB document class - no logic to test.")]
public class EtlImportErrorDetail
{
    /// <summary>The innermost exception's type name - the technical cause.</summary>
    public string? Type { get; set; }

    /// <summary>The stage running when the failure happened.</summary>
    public string? Stage { get; set; }

    /// <summary>The dataset the failure belongs to, when it belongs to one.</summary>
    public string? Dataset { get; set; }

    /// <summary>The object key being processed when the failure happened, when the failure is
    /// file-scoped - for example the raw source file that failed validation.</summary>
    public string? FileKey { get; set; }

    /// <summary>The 1-based position of the record that failed, when the failure is record-scoped.
    /// Unpopulated until the H/C/D/T parser reports position.</summary>
    public long? RecordNumber { get; set; }

    /// <summary>The value a validation rule expected, when the rule carries one - for example a
    /// trailer's declared record count.</summary>
    public string? Expected { get; set; }

    /// <summary>The value a validation rule observed, when the rule carries one.</summary>
    public string? Actual { get; set; }
}
