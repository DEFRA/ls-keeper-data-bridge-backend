using KeeperData.Bridge.Worker.NewPipelineUsage.Samples;
using KeeperData.Core.Ingestion;
using KeeperData.Core.Ingestion.Stages;
using KeeperData.Core.Pipeline;

namespace KeeperData.Bridge.Worker.NewPipelineUsage;

/// <summary>Demonstrates composing and running the pipeline with the in-memory sample services.
/// <see cref="RunAsync"/> uses the fluent factory; <see cref="ManuallyRunAsync"/> builds the same
/// pipeline without the extension methods, to show what the sugar expands to.</summary>
internal sealed class UsageExample
{
    private readonly SampleDataSetDefinitions _definitions = new();
    private readonly FilenamePasswordPolicy _passwordPolicy = new();
    private readonly PassthroughFileDecryptor _decryptor = new();
    private readonly SampleNormaliserFactory _normalisers = new();
    private readonly SampleDuckDbStagingWriter _stagingWriter = new();
    private readonly InMemoryDataBridgeStore _store = new();
    private readonly ConsoleWorkflowLog _log = new();

    public async Task RunAsync()
    {
        using var cancellationTokenSource = new CancellationTokenSource();

        var pipeline = FilePipelineFactory.Create(_definitions, _passwordPolicy, _decryptor, _normalisers, _stagingWriter);
        var context = new PipelineContext(_store, _definitions, _log);

        await new PipelineExecutor().RunAsync(pipeline, context, cancellationTokenSource.Token);
    }

    public async Task ManuallyRunAsync()
    {
        using var cancellationTokenSource = new CancellationTokenSource();

        // The same pipeline as FilePipelineFactory.Create, spelled out without the fluent
        // extensions: every .Discover()/.Decrypt()/... is builder.Then(new XStage(...)).
        var pipeline = PipelineBuilder
            .InputSource(new S3RawFolderSource(_definitions))
            .Then(new DiscoverStage(_definitions))
            .Then(new DecryptStage(_passwordPolicy, _decryptor))
            .Then(new NormaliseStage(_normalisers))
            .Then(new SnapshotStage())
            .Then(new LoadDuckDbStage(_stagingWriter))
            .Build();

        var context = new PipelineContext(_store, _definitions, _log);

        await new PipelineExecutor().RunAsync(pipeline, context, cancellationTokenSource.Token);
    }
}
