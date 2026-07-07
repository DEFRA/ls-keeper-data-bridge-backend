namespace KeeperData.Core.Ingestion.Contracts;

/// <summary>Structured per-stage telemetry, owned/called by the executor.</summary>
public interface IWorkflowLog
{
    void StageStarted(string stage);
    void StageSkipped(string stage, string reason);
    void StageSucceeded(string stage, TimeSpan elapsed);
    void StageFailed(string stage, Exception error);
}
