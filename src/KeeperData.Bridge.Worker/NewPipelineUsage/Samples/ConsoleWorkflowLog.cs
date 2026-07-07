using KeeperData.Core.Ingestion.Contracts;

namespace KeeperData.Bridge.Worker.NewPipelineUsage.Samples;

/// <summary>Writes stage telemetry to the console so a demo run is visible.</summary>
public sealed class ConsoleWorkflowLog : IWorkflowLog
{
    public void StageStarted(string stage) => Console.WriteLine($"[stage] {stage} started");
    public void StageSkipped(string stage, string reason) => Console.WriteLine($"[stage] {stage} skipped: {reason}");
    public void StageSucceeded(string stage, TimeSpan elapsed) => Console.WriteLine($"[stage] {stage} ok in {elapsed.TotalMilliseconds:n0} ms");
    public void StageFailed(string stage, Exception error) => Console.WriteLine($"[stage] {stage} FAILED: {error.Message}");
}
