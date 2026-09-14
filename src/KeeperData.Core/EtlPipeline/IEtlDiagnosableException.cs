using KeeperData.Core.EtlPipeline.Status;

namespace KeeperData.Core.EtlPipeline;

/// <summary>Marks an exception whose message was written for whoever reads an import's status,
/// rather than for whoever reads the log.
///
/// Import status reports the innermost exception, because that is the one that says what actually
/// went wrong. That is the wrong choice when a stage has caught an opaque technical failure and
/// wrapped it in an explanation: the explanation is what the reader needs, and the technical cause
/// belongs in the log. Marking the wrapper keeps its message in the status while the original
/// exception stays in the chain for the log.
///
/// A message on a marked exception is served to callers, so it must not carry a salt, a password,
/// a presigned URL or a configuration value.</summary>
public interface IEtlDiagnosableException
{
    /// <summary>Structured context persisted on the import document for a caller to read - which
    /// dataset, file or record the failure belongs to. Null when the exception carries nothing
    /// beyond its message. The same serving rules apply: no salts, passwords or presigned URLs.</summary>
    EtlImportErrorDetail? ErrorDetail => null;
}
