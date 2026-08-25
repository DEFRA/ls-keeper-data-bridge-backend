using System.Text.RegularExpressions;
using Amazon.S3.Model;
using DuckDB.NET.Data;
using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.ETL.Export;
using KeeperData.Core.ETL.Models;
using KeeperData.Core.Storage;
using KeeperData.Core.Telemetry;
using KeeperData.Infrastructure.ETL;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Scenarios;

[Collection("LocalStackAndDuckDb"), Trait("Dependence", "docker")]
public partial class CphExportPipelineIntegrationTests : IAsyncLifetime
{
    [GeneratedRegex(@"^\d{2}/\d{3}/\d{4}$")]
    private static partial Regex CphFormatRegex();

    private readonly ITestOutputHelper _output;
    private readonly LocalStackFixture _localStackFixture;
    private readonly DuckDbStubFixture _duckDbStubFixture;
    private ServiceProvider? _serviceProvider;

    public CphExportPipelineIntegrationTests(
        ITestOutputHelper output,
        LocalStackFixture localStackFixture,
        DuckDbStubFixture duckDbStubFixture)
    {
        _output = output;
        _localStackFixture = localStackFixture;
        _duckDbStubFixture = duckDbStubFixture;
    }

    public async Task InitializeAsync()
    {
        await _duckDbStubFixture.UploadToS3Async(
            _localStackFixture.S3Client, LocalStackFixture.TestBucket);

        _serviceProvider = BuildServices();
    }

    public async Task DisposeAsync()
    {
        if (_serviceProvider != null)
            await _serviceProvider.DisposeAsync();
    }

    [Fact]
    public async Task ExportAsync_ShouldExtractDistinctCphsAndUploadSqlite()
    {
        _output.WriteLine("=== CPH Export Pipeline Integration Test ===");

        var exportService = _serviceProvider!.GetRequiredService<ICphExportService>();

        var result = await exportService.ExportAsync(_duckDbStubFixture.StagingKey);
        _output.WriteLine($"Export completed: {result.RowCount} rows -> {result.SqliteKey}");

        result.RowCount.Should().BeGreaterThan(0, "should extract at least one distinct CPH");
        result.SqliteKey.Should().StartWith("views/cphs_");
        result.SqliteKey.Should().EndWith(".sqlite");
        result.SourceDuckDbKey.Should().Be(_duckDbStubFixture.StagingKey);

        var s3Objects = await _localStackFixture.S3Client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = LocalStackFixture.TestBucket,
            Prefix = "views/cphs_"
        });
        s3Objects.S3Objects.Should().ContainSingle(o => o.Key == result.SqliteKey);
        _output.WriteLine($"Verified SQLite exists in S3: {result.SqliteKey}");
    }

    [Fact]
    public async Task ExportAsync_ShouldProduceSqliteWithCorrectSchema()
    {
        _output.WriteLine("=== CPH Export SQLite Schema Verification ===");

        var exportService = _serviceProvider!.GetRequiredService<ICphExportService>();

        var result = await exportService.ExportAsync(_duckDbStubFixture.StagingKey);

        var tempPath = Path.Combine(Path.GetTempPath(), $"cph_schema_test_{Guid.NewGuid():N}.sqlite");
        try
        {
            var response = await _localStackFixture.S3Client.GetObjectAsync(
                LocalStackFixture.TestBucket, result.SqliteKey);
            await using (var fileStream = File.Create(tempPath))
            {
                await response.ResponseStream.CopyToAsync(fileStream);
            }

            using var connection = new SqliteConnection($"Data Source={tempPath};Mode=ReadOnly");
            connection.Open();

            using var tableCmd = connection.CreateCommand();
            tableCmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table'";
            using var reader = tableCmd.ExecuteReader();
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("cphs");
            _output.WriteLine("Verified 'cphs' table exists");

            using var countCmd = connection.CreateCommand();
            countCmd.CommandText = "SELECT COUNT(*) FROM cphs";
            var count = Convert.ToInt32(countCmd.ExecuteScalar());
            count.Should().Be(result.RowCount);
            _output.WriteLine($"Verified row count matches: {count}");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (File.Exists(tempPath)) File.Delete(tempPath);
        }
    }

    [Fact]
    public async Task ExportAsync_ShouldProduceValidDistinctCphValues()
    {
        _output.WriteLine("=== CPH Export Data Integrity Verification ===");

        var exportService = _serviceProvider!.GetRequiredService<ICphExportService>();
        var result = await exportService.ExportAsync(_duckDbStubFixture.StagingKey);

        var tempSqlite = Path.Combine(Path.GetTempPath(), $"cph_data_test_{Guid.NewGuid():N}.sqlite");
        var tempDuckDb = Path.Combine(Path.GetTempPath(), $"cph_data_test_{Guid.NewGuid():N}.duckdb");
        try
        {
            var sqliteResponse = await _localStackFixture.S3Client.GetObjectAsync(
                LocalStackFixture.TestBucket, result.SqliteKey);
            await using (var fs = File.Create(tempSqlite))
                await sqliteResponse.ResponseStream.CopyToAsync(fs);

            await using (var duckDbStream = await _duckDbStubFixture.DownloadFromS3Async(
                _localStackFixture.S3Client, LocalStackFixture.TestBucket))
            await using (var fs = File.Create(tempDuckDb))
                await duckDbStream.CopyToAsync(fs);

            // Get CPHs from SQLite output
            var sqliteCphs = new List<string>();
            using (var conn = new SqliteConnection($"Data Source={tempSqlite};Mode=ReadOnly"))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT CPH FROM cphs ORDER BY CPH";
                using var reader = cmd.ExecuteReader();
                while (reader.Read()) sqliteCphs.Add(reader.GetString(0));
            }

            // Get distinct CPHs from DuckDB source
            var duckDbCphs = new List<string>();
            using (var conn = new DuckDBConnection($"Data Source={tempDuckDb}"))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT DISTINCT CPH FROM sam_cph_holdings WHERE CPH IS NOT NULL AND CPH <> '' ORDER BY CPH";
                using var reader = cmd.ExecuteReader();
                while (reader.Read()) duckDbCphs.Add(reader.GetString(0));
            }

            sqliteCphs.Should().BeEquivalentTo(duckDbCphs, "SQLite output should match distinct CPHs from DuckDB");
            sqliteCphs.Should().OnlyContain(c => CphFormatRegex().IsMatch(c), "all CPHs should be NN/NNN/NNNN format");
            sqliteCphs.Should().BeInAscendingOrder("CPHs should be sorted");
            _output.WriteLine($"Verified {sqliteCphs.Count} CPHs match between DuckDB source and SQLite output");
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (File.Exists(tempSqlite)) File.Delete(tempSqlite);
            if (File.Exists(tempDuckDb)) File.Delete(tempDuckDb);
        }
    }

    [Fact]
    public async Task ExportAsync_ShouldBeIdempotent_WhenRunTwice()
    {
        _output.WriteLine("=== CPH Export Idempotency Test ===");

        var exportService = _serviceProvider!.GetRequiredService<ICphExportService>();

        var result1 = await exportService.ExportAsync(_duckDbStubFixture.StagingKey);
        _output.WriteLine($"First export: {result1.RowCount} rows -> {result1.SqliteKey}");

        var result2 = await exportService.ExportAsync(_duckDbStubFixture.StagingKey);
        _output.WriteLine($"Second export: {result2.RowCount} rows -> {result2.SqliteKey}");

        result2.SqliteKey.Should().Be(result1.SqliteKey, "idempotent export should produce same key");
        result2.RowCount.Should().Be(result1.RowCount, "row count should be identical");

        var objects = await _localStackFixture.S3Client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = LocalStackFixture.TestBucket,
            Prefix = result1.SqliteKey
        });
        objects.S3Objects.Should().HaveCount(1, "only one SQLite file should exist (no duplicates)");
        _output.WriteLine("Verified idempotent export did not create duplicate files");
    }

    [Fact]
    public async Task ExportAsync_WithAutoDiscovery_ShouldFindLatestDuckDb()
    {
        _output.WriteLine("=== CPH Export Auto-Discovery Test ===");

        var exportService = _serviceProvider!.GetRequiredService<ICphExportService>();

        var result = await exportService.ExportAsync();
        _output.WriteLine($"Auto-discovery export: {result.RowCount} rows from {result.SourceDuckDbKey} -> {result.SqliteKey}");

        result.SourceDuckDbKey.Should().Be(_duckDbStubFixture.StagingKey,
            "should auto-discover the uploaded DuckDB stub");
        result.RowCount.Should().BeGreaterThan(0);
        result.SqliteKey.Should().Contain("cphs_");
    }

    [Fact]
    public async Task ExportStatusService_ShouldPersistAndRetrieveStatus()
    {
        _output.WriteLine("=== Export Status Service Integration Test ===");

        var statusService = _serviceProvider!.GetRequiredService<ICphExportStatusService>();

        var exportId = Guid.NewGuid();
        var created = await statusService.CreateAsync(exportId, "staging/test.duckdb");
        _output.WriteLine($"Created status: {created.ExportId} = {created.Status}");

        created.ExportId.Should().Be(exportId);
        created.Status.Should().Be(ExportStatusType.Queued);

        var retrieved = await statusService.GetAsync(exportId);
        retrieved.Should().NotBeNull();
        retrieved!.ExportId.Should().Be(exportId);
        retrieved.Status.Should().Be(ExportStatusType.Queued);
        _output.WriteLine("Verified status persisted and retrieved from S3");

        retrieved.Status = ExportStatusType.Running;
        retrieved.StartedAt = DateTime.UtcNow;
        await statusService.UpdateAsync(retrieved);

        var updated = await statusService.GetAsync(exportId);
        updated!.Status.Should().Be(ExportStatusType.Running);
        updated.StartedAt.Should().NotBeNull();
        _output.WriteLine("Verified status updated to Running");

        updated.Status = ExportStatusType.Succeeded;
        updated.CompletedAt = DateTime.UtcNow;
        updated.RowCount = 42;
        updated.SqlitePath = "views/cphs_test.sqlite";
        await statusService.UpdateAsync(updated);

        var completed = await statusService.GetAsync(exportId);
        completed!.Status.Should().Be(ExportStatusType.Succeeded);
        completed.RowCount.Should().Be(42);
        _output.WriteLine("Verified full lifecycle: Queued -> Running -> Succeeded");
    }

    [Fact]
    public async Task GetLatestRunningAsync_ShouldFindActiveExport()
    {
        _output.WriteLine("=== GetLatestRunning Integration Test ===");

        var statusService = _serviceProvider!.GetRequiredService<ICphExportStatusService>();

        var noRunning = await statusService.GetLatestRunningAsync();
        _output.WriteLine($"Before creating: latest running = {(noRunning is null ? "null" : noRunning.ExportId.ToString())}");

        var exportId = Guid.NewGuid();
        await statusService.CreateAsync(exportId, "staging/test.duckdb");

        var running = await statusService.GetLatestRunningAsync();
        running.Should().NotBeNull();
        running!.ExportId.Should().Be(exportId);
        running.Status.Should().Be(ExportStatusType.Queued);
        _output.WriteLine($"Found queued export: {running.ExportId}");

        running.Status = ExportStatusType.Succeeded;
        running.CompletedAt = DateTime.UtcNow;
        await statusService.UpdateAsync(running);

        var afterComplete = await statusService.GetLatestRunningAsync();
        afterComplete.Should().BeNull("no running/queued exports should remain");
        _output.WriteLine("Verified no running exports after completion");
    }

    [Fact]
    public async Task FullPipeline_TriggerExportAndTrackStatus()
    {
        _output.WriteLine("=== Full Pipeline Integration Test ===");

        var exportService = _serviceProvider!.GetRequiredService<ICphExportService>();
        var statusService = _serviceProvider!.GetRequiredService<ICphExportStatusService>();

        // 1. Create status
        var exportId = Guid.NewGuid();
        var status = await statusService.CreateAsync(exportId, _duckDbStubFixture.StagingKey);
        _output.WriteLine($"Step 1: Created export {exportId} with status {status.Status}");

        // 2. Mark as running
        status.Status = ExportStatusType.Running;
        status.StartedAt = DateTime.UtcNow;
        await statusService.UpdateAsync(status);
        _output.WriteLine("Step 2: Marked as Running");

        // 3. Run export
        var result = await exportService.ExportAsync(_duckDbStubFixture.StagingKey);
        _output.WriteLine($"Step 3: Export completed — {result.RowCount} rows -> {result.SqliteKey}");

        // 4. Mark as succeeded
        status.Status = ExportStatusType.Succeeded;
        status.CompletedAt = DateTime.UtcNow;
        status.SqlitePath = result.SqliteKey;
        status.RowCount = result.RowCount;
        await statusService.UpdateAsync(status);
        _output.WriteLine("Step 4: Marked as Succeeded");

        // 5. Verify final state
        var finalStatus = await statusService.GetAsync(exportId);
        finalStatus!.Status.Should().Be(ExportStatusType.Succeeded);
        finalStatus.RowCount.Should().Be(result.RowCount);
        finalStatus.SqlitePath.Should().Be(result.SqliteKey);
        finalStatus.StartedAt.Should().NotBeNull();
        finalStatus.CompletedAt.Should().NotBeNull();
        finalStatus.CompletedAt.Should().BeAfter(finalStatus.StartedAt!.Value);
        _output.WriteLine("Step 5: Verified full pipeline state");

        // 6. Verify SQLite is downloadable from S3
        var metadata = await _localStackFixture.S3Client.GetObjectMetadataAsync(
            LocalStackFixture.TestBucket, result.SqliteKey);
        metadata.ContentLength.Should().BeGreaterThan(0);
        _output.WriteLine($"Step 6: Verified SQLite downloadable ({metadata.ContentLength} bytes)");

        _output.WriteLine("=== Full Pipeline Integration Test PASSED ===");
    }

    private ServiceProvider BuildServices()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());

        var storageConfig = new StorageConfiguration
        {
            InternalStorage = new StorageConfigurationDetails
            {
                BucketName = LocalStackFixture.TestBucket,
                HealthcheckEnabled = false
            },
            ExternalStorage = new StorageWithCredentialsConfiguration
            {
                BucketName = LocalStackFixture.TestBucket,
                AccessKeySecretName = "not-used",
                SecretKeySecretName = "not-used",
                HealthcheckEnabled = false
            },
            SourceInternalPrefix = "",
            TargetInternalPrefix = "",
            SourceExternalPrefix = ""
        };
        services.AddSingleton(storageConfig);

        var s3ClientFactory = new S3ClientFactory();
        s3ClientFactory.RegisterMockClient<InternalStorageClient>(
            LocalStackFixture.TestBucket, _localStackFixture.S3Client);
        services.AddSingleton<IS3ClientFactory>(s3ClientFactory);

        services.AddTransient<IBlobStorageServiceFactory, S3BlobStorageServiceFactory>();
        services.AddSingleton(Mock.Of<IApplicationMetrics>());
        services.AddScoped<ICphExportService, CphExportService>();
        services.AddScoped<ICphExportStatusService, CphExportStatusService>();

        return services.BuildServiceProvider();
    }
}
