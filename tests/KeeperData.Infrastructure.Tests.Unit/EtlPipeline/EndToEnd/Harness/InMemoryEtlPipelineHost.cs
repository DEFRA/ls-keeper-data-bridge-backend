using System.Collections.Immutable;
using System.Text;
using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Setup;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.EtlPipeline.Views;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd.Harness;

/// <summary>
/// Hosts the ETL pipeline with no docker and no network: in-memory blob storage, everything else
/// real. The stage graph, the executor, the catalogue, AES, Parquet and the delta merge engine are
/// the production types, resolved through the production <c>AddEtlPipeline</c> registration, so a
/// stage that stops being wired up fails these tests.
///
/// Its one job is composition. Fixture content lives in <see cref="EtlFixtures"/> and assertions
/// live in the test class, so this type changes only when the pipeline's dependencies change.
/// </summary>
public sealed class InMemoryEtlPipelineHost : IDisposable
{
    /// <summary>Test-only salt. Never a production value.</summary>
    public const string AesSalt = "Jr8Lm2PXzd7qNbVyWutRfGBxhkHTpE";

    private readonly ServiceProvider _services;

    private InMemoryEtlPipelineHost(
        ServiceProvider services,
        InMemorySourceStorage source,
        InMemoryEtlPipelineStorageProvider folders,
        FakeTimeProvider timeProvider)
    {
        _services = services;

        Source = source;
        Folders = folders;
        TimeProvider = timeProvider;
    }

    /// <summary>The container the pipeline discovers encrypted source files from.</summary>
    public InMemorySourceStorage Source { get; }

    /// <summary>The ETL folders the pipeline materialises into.</summary>
    public InMemoryEtlPipelineStorageProvider Folders { get; }

    public FakeTimeProvider TimeProvider { get; }

    /// <param name="now">Discovery is a lookback window ending "today", so the clock decides which
    /// source files a run can see.</param>
    /// <param name="definitions">The datasets the run discovers. Narrowed so a test is not paying
    /// for nine prefixes it does not assert on.</param>
    /// <param name="stagingDatabaseWriter">Omit for the recording writer; pass the real DuckDB
    /// writer to cover the SQL as well.</param>
    /// <param name="sqliteViewWriter">Omit for the recording writer; pass the real DuckDB writer to
    /// cover the read-model transformation as well.</param>
    public static InMemoryEtlPipelineHost Create(
        DateTimeOffset now,
        IReadOnlyList<DataSetDefinition> definitions,
        IStagingDatabaseWriter? stagingDatabaseWriter = null,
        ISqliteViewWriter? sqliteViewWriter = null)
    {
        ArgumentNullException.ThrowIfNull(definitions);

        var source = new InMemorySourceStorage();
        var folders = new InMemoryEtlPipelineStorageProvider();
        var timeProvider = new FakeTimeProvider(now);

        var services = new ServiceCollection();

        // No providers registered, so the pipeline's logging is silent in CI.
        services.AddLogging();

        services.AddSingleton<TimeProvider>(timeProvider);

        services.AddSingleton<IConfiguration>(new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["AesSalt"] = AesSalt })
            .Build());

        services.AddSingleton<IDataSetDefinitions>(new SelectedDataSetDefinitions(definitions));

        // The only substitutions. Everything below this line is the production type.
        services.AddSingleton(source);
        services.AddSingleton<IBlobStorageServiceFactory, InMemoryBlobStorageServiceFactory>();
        services.AddSingleton(folders);
        services.AddSingleton<IEtlPipelineStorageProvider>(folders);
        services.AddSingleton(stagingDatabaseWriter ?? new RecordingStagingDatabaseWriter());
        services.AddSingleton(sqliteViewWriter ?? new RecordingSqliteViewWriter());

        services.AddSingleton<IPasswordSaltService, PasswordSaltService>();
        services.AddSingleton<IAesCryptoTransform, AesCryptoTransform>();
        services.AddTransient<IExternalCatalogueServiceFactory, ExternalCatalogueServiceFactory>();

        services.AddEtlPipeline();

        return new InMemoryEtlPipelineHost(services.BuildServiceProvider(), source, folders, timeProvider);
    }

    /// <summary>Runs the whole pipeline once, the way the coordinator runs it.</summary>
    public async Task<Guid> RunAsync(
        int lookbackDays = 30,
        string? dataset = null,
        Guid? runId = null,
        CancellationToken cancellationToken = default)
    {
        using var scope = _services.CreateScope();

        var factory = scope.ServiceProvider.GetRequiredService<IEtlPipelineFactory>();
        var executor = scope.ServiceProvider.GetRequiredService<IPipelineExecutor>();

        var id = runId ?? Guid.NewGuid();

        await executor.RunAsync(
            factory.Create(),
            new EtlPipelineContext(id, BlobStorageSources.External, lookbackDays, dataset),
            cancellationToken);

        return id;
    }

    /// <summary>Encrypts PSV content the way the source system does and puts it in the source
    /// container. The decrypt stage derives the password from the object key, so the content is
    /// encrypted against the key it is stored under.</summary>
    /// <param name="salt">Defaults to the salt this host is configured with. Pass another to
    /// produce the file you get when it was encrypted for a different environment.</param>
    public async Task<string> PutEncryptedSourceFileAsync(string fileName, string psvContent, string? salt = null)
    {
        var crypto = _services.GetRequiredService<IAesCryptoTransform>();

        using var plaintext = new MemoryStream(Encoding.UTF8.GetBytes(psvContent));
        using var encrypted = new MemoryStream();

        await crypto.EncryptStreamAsync(plaintext, encrypted, fileName, salt ?? AesSalt, plaintext.Length);

        Source.Seed(fileName, encrypted.ToArray());

        return fileName;
    }

    /// <summary>Downloads an object from an ETL folder to a local file, for readers needing a path.</summary>
    public async Task<string> DownloadToTempAsync(string folder, string key, string extension)
    {
        var path = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}{extension}");
        await File.WriteAllBytesAsync(path, Folders.Folder(folder).BytesOf(key));

        return path;
    }

    public void Dispose() => _services.Dispose();

    /// <summary>The dataset definitions the pipeline discovers, narrowed to those under test.</summary>
    private sealed class SelectedDataSetDefinitions(IReadOnlyList<DataSetDefinition> definitions) : IDataSetDefinitions
    {
        public ImmutableArray<DataSetDefinition> All { get; } = [.. definitions];
    }
}
