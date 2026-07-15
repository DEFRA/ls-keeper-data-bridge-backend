using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage.Dtos;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

/// <summary>Runs a single stage in isolation. Hand it a stage and a list of inputs, get back the list
/// of outputs it produced. No pipeline, no executor, no other stages.
///
/// Standard pattern for a stage test:
///   var output = await StageRunner.RunAsync(new MyStage(), [ input1, input2 ]);
///   output.Should().Be(...);
/// </summary>
public static class StageRunner
{
    public static async Task<List<TOut>> RunAsync<TIn, TOut>(
        IStage<TIn, TOut> stage,
        IReadOnlyList<TIn> inputs,
        IPipelineContext? context = null,
        CancellationToken cancellationToken = default)
    {
        var results = new List<TOut>();
        await foreach (var item in stage.RunAsync(ToStream(inputs), context ?? Context(), cancellationToken))
        {
            results.Add(item);
        }
        return results;
    }

    /// <summary>For a source stage (no input), e.g. S3RawFolderSource.</summary>
    public static async Task<List<TOut>> RunSourceAsync<TOut>(
        ISourceStage<TOut> source,
        IPipelineContext? context = null,
        CancellationToken cancellationToken = default)
    {
        var results = new List<TOut>();
        await foreach (var item in source.RunAsync(context ?? Context(), cancellationToken))
        {
            results.Add(item);
        }
        return results;
    }

    public static EtlPipelineContext Context(string sourceType = "external", int lookbackDays = 0) =>
        new(Guid.NewGuid(), sourceType, lookbackDays);

    // Shared test-data builders, so every stage test describes inputs the same way.

    public static DataSetDefinition Definition(string name = "SAM_CPH") =>
        new(name, $"{name}_{{0}}", ["cph"], "CHANGE_TYPE", []);

    public static EtlFile File(string key, DateTimeOffset? timestamp = null) =>
        new(new StorageObjectInfo
        {
            Container = "external",
            Key = key,
            StorageUri = new Uri($"s3://external/{key}")
        }, timestamp ?? DateTimeOffset.UtcNow);

    public static DiscoveredFile Discovered(string dataset, string key) =>
        new(Definition(dataset), File(key));

    public static DiscoveredFileSet DiscoveredSet(string dataset, params string[] keys) =>
        new(Definition(dataset), [.. keys.Select(k => File(k))]);

    private static async IAsyncEnumerable<T> ToStream<T>(IReadOnlyList<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }
        await Task.CompletedTask;
    }
}
