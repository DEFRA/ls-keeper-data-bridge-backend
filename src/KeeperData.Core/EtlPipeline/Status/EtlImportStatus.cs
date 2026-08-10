namespace KeeperData.Core.EtlPipeline.Status;

/// <summary>Lifecycle of an ETL import, as reported to the polling API.</summary>
public enum EtlImportStatus
{
    /// <summary>Accepted and the lock is held, but the pipeline has not started yet.</summary>
    Queued,

    Running,

    Succeeded,

    Failed,

    /// <summary>The request was refused before any work began.</summary>
    Rejected
}
