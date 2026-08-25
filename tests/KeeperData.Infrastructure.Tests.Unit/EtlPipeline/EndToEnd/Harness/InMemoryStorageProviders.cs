using System.Collections.Concurrent;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd.Harness;

/// <summary>
/// The source container the pipeline discovers and decrypts from. A named type rather than a bare
/// <see cref="InMemoryBlobStorage"/> so it can be injected and resolved unambiguously alongside the
/// ETL folders.
/// </summary>
public sealed class InMemorySourceStorage() : InMemoryBlobStorage("source");

/// <summary>
/// In-memory <see cref="IEtlPipelineStorageProvider"/>, one container per ETL folder.
/// Substitutes for S3 without changing a single stage.
/// </summary>
public sealed class InMemoryEtlPipelineStorageProvider : IEtlPipelineStorageProvider
{
    private readonly ConcurrentDictionary<string, InMemoryBlobStorage> _folders = new(StringComparer.Ordinal);

    /// <summary>The folder's storage, for assertions. Creates it if the pipeline has not yet.</summary>
    public InMemoryBlobStorage Folder(string folder) => _folders.GetOrAdd(folder, name => new InMemoryBlobStorage(name));

    public IBlobStorageService ForFolder(string folder) => Folder(folder);
}

/// <summary>
/// Hands every caller the same in-memory source container. The pipeline only ever asks for the
/// source, so the destination-side members throw rather than quietly return an empty container that
/// would make a wrong test pass.
/// </summary>
public sealed class InMemoryBlobStorageServiceFactory(InMemorySourceStorage source) : IBlobStorageServiceFactory
{
    public IBlobStorageServiceReadOnly GetSource(string type) => source;

    public IBlobStorageServiceReadOnly GetSourceExternal() => source;

    public IBlobStorageService GetSourceInternal() => source;

    public IBlobStorageService Get() => throw new NotSupportedException(NotUsed(nameof(Get)));

    public IBlobStorageService GetCleanseReportsBlobService()
        => throw new NotSupportedException(NotUsed(nameof(GetCleanseReportsBlobService)));

    private static string NotUsed(string member)
        => $"{member} is not reached by the ETL pipeline. If this throws, a stage has started using it and the harness needs updating.";
}
