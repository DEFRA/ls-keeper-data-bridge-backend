namespace KeeperData.Core.EtlPipeline.Snapshots;

public sealed record DeltaMergeSource(string Key, Func<CancellationToken, Task<Stream>> Open);
