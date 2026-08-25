using System.Collections.Immutable;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Setup;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.EtlPipeline.Views;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.EtlPipeline.Staging;
using KeeperData.Infrastructure.EtlPipeline.Storage;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

/// <summary>
/// Hosts the ETL pipeline over a bucket of its own in LocalStack, wired the way the
/// application wires it: real crypto, real catalogue, real storage, real Parquet, real DuckDB.
/// Nothing from the legacy Mongo ETL is registered, so a run here cannot reach it.
///
/// Each instance owns its bucket, so the pipeline folders start empty and tests do not see one
/// another's snapshots.
/// </summary>
public sealed class EtlPipelineTestHost : IAsyncDisposable
{
    public const string SourcePrefix = "litprd";
    public const string AesSalt = "Jr8Lm2PXzd7qNbVyWutRfGBxhkHTpE";

    private readonly IAmazonS3 _s3Client;
    private readonly ServiceProvider _services;
    private readonly FakeTimeProvider _timeProvider;

    private EtlPipelineTestHost(
        IAmazonS3 s3Client,
        string bucketName,
        ServiceProvider services,
        FakeTimeProvider timeProvider,
        IReadOnlyList<DataSetDefinition> definitions)
    {
        _s3Client = s3Client;
        _services = services;
        _timeProvider = timeProvider;

        BucketName = bucketName;
        Definitions = definitions;
    }

    public string BucketName { get; }

    /// <summary>Every dataset this host discovers.</summary>
    public IReadOnlyList<DataSetDefinition> Definitions { get; }

    /// <summary>The dataset under test, for the single-dataset tests. Throws when the host was
    /// created with several, because in that case "the" dataset is not a meaningful thing to ask for.</summary>
    public DataSetDefinition Definition => Definitions.Count == 1
        ? Definitions[0]
        : throw new InvalidOperationException(
            $"This host discovers {Definitions.Count} datasets. Use {nameof(Definitions)} instead.");

    /// <summary>Creates the bucket and wires the pipeline against it.</summary>
    /// <param name="now">Discovery is a date window ending "today", so the clock decides which source
    /// files a run can see. Fixed here so tests can use realistic source timestamps.</param>
    /// <param name="stagingDatabaseWriter">Replaces the DuckDB writer, for failure scenarios.</param>
    /// <param name="statusStore">Records import status for the run. Omitted, no status is written -
    /// the pipeline does not depend on it.</param>
    public static Task<EtlPipelineTestHost> CreateAsync(
        IAmazonS3 s3Client,
        DateTimeOffset now,
        DataSetDefinition? definition = null,
        IStagingDatabaseWriter? stagingDatabaseWriter = null,
        IEtlImportStatusStore? statusStore = null,
        ISqliteViewWriter? sqliteViewWriter = null)
        => CreateAsync(
            s3Client,
            now,
            [definition ?? StandardDataSetDefinitionsBuilder.Build().SamCPHHolding],
            stagingDatabaseWriter,
            statusStore,
            sqliteViewWriter);

    /// <summary>Creates the bucket and wires the pipeline against it for several datasets at once,
    /// so a run can be observed routing each dataset into its own prefix.</summary>
    public static async Task<EtlPipelineTestHost> CreateAsync(
        IAmazonS3 s3Client,
        DateTimeOffset now,
        IReadOnlyList<DataSetDefinition> definitions,
        IStagingDatabaseWriter? stagingDatabaseWriter = null,
        IEtlImportStatusStore? statusStore = null,
        ISqliteViewWriter? sqliteViewWriter = null)
    {
        ArgumentNullException.ThrowIfNull(definitions);

        if (definitions.Count == 0)
        {
            throw new ArgumentException("At least one dataset is needed, or the run has nothing to discover.", nameof(definitions));
        }

        var bucketName = $"etl-e2e-{Guid.NewGuid():N}";
        await s3Client.PutBucketAsync(new PutBucketRequest { BucketName = bucketName, UseClientRegion = true });

        var timeProvider = new FakeTimeProvider(now);

        var services = BuildServices(
            s3Client, bucketName, definitions, timeProvider, stagingDatabaseWriter, statusStore, sqliteViewWriter);

        return new EtlPipelineTestHost(s3Client, bucketName, services, timeProvider, definitions);
    }

    private static ServiceProvider BuildServices(
        IAmazonS3 s3Client,
        string bucketName,
        IReadOnlyList<DataSetDefinition> definitions,
        TimeProvider timeProvider,
        IStagingDatabaseWriter? stagingDatabaseWriter,
        IEtlImportStatusStore? statusStore,
        ISqliteViewWriter? sqliteViewWriter)
    {
        var services = new ServiceCollection();

        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));

        services.AddSingleton(timeProvider);

        services.AddSingleton<IConfiguration>(new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["AesSalt"] = AesSalt })
            .Build());

        // Only the datasets under test, so discovery does not list twelve empty prefixes per run.
        services.AddSingleton<IDataSetDefinitions>(new SelectedDataSetDefinitions(definitions));

        services.AddSingleton(new StorageConfiguration
        {
            ExternalStorage = new StorageWithCredentialsConfiguration
            {
                BucketName = bucketName,
                AccessKeySecretName = "not-used",
                SecretKeySecretName = "not-used",
                HealthcheckEnabled = false
            },
            InternalStorage = new StorageConfigurationDetails
            {
                BucketName = bucketName,
                HealthcheckEnabled = false
            },
            SourceExternalPrefix = SourcePrefix,
            SourceInternalPrefix = SourcePrefix,
            TargetInternalPrefix = "dest"
        });

        var s3ClientFactory = new S3ClientFactory();
        s3ClientFactory.RegisterMockClient<ExternalStorageClient>(bucketName, s3Client);
        s3ClientFactory.RegisterMockClient<InternalStorageClient>(bucketName, s3Client);
        services.AddSingleton<IS3ClientFactory>(s3ClientFactory);

        services.AddTransient<IBlobStorageServiceFactory, S3BlobStorageServiceFactory>();
        services.AddSingleton<IPasswordSaltService, PasswordSaltService>();
        services.AddSingleton<IAesCryptoTransform, AesCryptoTransform>();
        services.AddTransient<IExternalCatalogueServiceFactory, ExternalCatalogueServiceFactory>();

        services.AddTransient<IEtlPipelineStorageProvider, S3EtlPipelineStorageProvider>();

        if (stagingDatabaseWriter is null)
        {
            services.AddScoped<IStagingDatabaseWriter, DuckDbStagingDatabaseWriter>();
        }
        else
        {
            services.AddSingleton(stagingDatabaseWriter);
        }

        services.AddSingleton<ISqliteViewWriter>(sqliteViewWriter ?? new RecordingSqliteViewWriter());

        services.AddEtlPipeline();

        if (statusStore is not null)
        {
            services.AddSingleton(statusStore);
            services.AddScoped<IPipelineRunObserver, EtlImportStatusObserver>();
        }

        return services.BuildServiceProvider();
    }

    /// <summary>Runs the whole pipeline once, as the host would.</summary>
    public async Task<Guid> RunPipelineAsync(
        int lookbackDays = 30,
        Guid? runId = null,
        string? dataset = null,
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

    /// <summary>Moves the run clock, so a later run can see files stamped after the previous one.</summary>
    public void SetNow(DateTimeOffset now) => _timeProvider.SetUtcNow(now);

    /// <summary>Encrypts PSV content the way the source system does and puts it in the source folder,
    /// under the dataset's own naming convention.
    ///
    /// <paramref name="salt"/> defaults to the salt this host is configured with; pass a different
    /// one to produce the file a caller gets when it was encrypted for another environment.</summary>
    public async Task<string> PutEncryptedSourceFileAsync(string fileName, string psvContent, string? salt = null)
    {
        var crypto = new AesCryptoTransform();
        var plaintext = new MemoryStream(Encoding.UTF8.GetBytes(psvContent));
        var encrypted = new MemoryStream();

        // The decrypt stage derives the password from the object key, so encrypt against that key.
        await crypto.EncryptStreamAsync(plaintext, encrypted, fileName, salt ?? AesSalt, plaintext.Length);

        encrypted.Position = 0;

        await _s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = BucketName,
            Key = $"{SourcePrefix}/{fileName}",
            InputStream = encrypted,
            ContentType = "application/octet-stream"
        });

        return fileName;
    }

    /// <summary>Every key under one of the pipeline folders, relative to that folder.</summary>
    public async Task<IReadOnlyList<string>> ListFolderAsync(string folder)
    {
        var response = await _s3Client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = BucketName,
            Prefix = $"{folder}/"
        });

        return [.. (response.S3Objects ?? [])
            .Select(o => o.Key[(folder.Length + 1)..])
            .OrderBy(key => key, StringComparer.Ordinal)];
    }

    public async Task<string> ReadTextAsync(string folder, string key)
    {
        using var response = await _s3Client.GetObjectAsync(BucketName, $"{folder}/{key}");
        using var reader = new StreamReader(response.ResponseStream);

        return await reader.ReadToEndAsync();
    }

    /// <summary>Downloads an object to a local file, for the readers that need a path.</summary>
    public async Task<string> DownloadToTempAsync(string folder, string key, string extension)
    {
        var path = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}{extension}");

        using var response = await _s3Client.GetObjectAsync(BucketName, $"{folder}/{key}");
        await using (var file = File.Create(path))
        {
            await response.ResponseStream.CopyToAsync(file);
        }

        return path;
    }

    public async ValueTask DisposeAsync()
    {
        await _services.DisposeAsync();

        try
        {
            var objects = await _s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = BucketName });

            foreach (var s3Object in objects.S3Objects ?? [])
            {
                await _s3Client.DeleteObjectAsync(BucketName, s3Object.Key);
            }

            await _s3Client.DeleteBucketAsync(BucketName);
        }
        catch (AmazonS3Exception)
        {
            // The bucket is disposable test scaffolding; a failure to tidy it up must not fail a test.
        }
    }

    /// <summary>The dataset definitions the pipeline discovers, narrowed to those under test.</summary>
    private sealed class SelectedDataSetDefinitions(IReadOnlyList<DataSetDefinition> definitions) : IDataSetDefinitions
    {
        public ImmutableArray<DataSetDefinition> All { get; } = [.. definitions];
    }
}
