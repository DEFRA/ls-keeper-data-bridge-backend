using DuckDB.NET.Data;
using FluentAssertions;
using KeeperData.Core.ETL.Export;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using KeeperData.Infrastructure.ETL;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.ETL;

public class PipelineCphExportServiceTests : IDisposable
{
    private readonly Mock<IBlobStorageServiceFactory> _storageFactoryMock;
    private readonly Mock<IBlobStorageService> _internalStorageMock;
    private readonly PipelineCphExportService _sut;
    private readonly string _tempDir;

    public PipelineCphExportServiceTests()
    {
        _storageFactoryMock = new Mock<IBlobStorageServiceFactory>();
        _internalStorageMock = new Mock<IBlobStorageService>();

        _storageFactoryMock
            .Setup(f => f.GetSourceInternal())
            .Returns(_internalStorageMock.Object);

        var executor = new PipelineExecutor(NullLogger<PipelineExecutor>.Instance);
        _sut = new PipelineCphExportService(executor, _storageFactoryMock.Object, NullLoggerFactory.Instance);

        _tempDir = Path.Combine(Path.GetTempPath(), "cph-export-pipeline-tests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task ExportAsync_WithSourceKey_DownloadsProcessesAndUploads()
    {
        var duckDbPath = CreateTestDuckDb(["01/001/0001", "02/002/0002", "01/001/0001"]);
        var duckDbBytes = await File.ReadAllBytesAsync(duckDbPath);
        var sourceKey = "staging/keeper_data_bridge_20260623T120000Z.duckdb";

        _internalStorageMock
            .Setup(s => s.OpenReadAsync(sourceKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new MemoryStream(duckDbBytes));

        _internalStorageMock
            .Setup(s => s.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var result = await _sut.ExportAsync(sourceKey);

        result.SourceDuckDbKey.Should().Be(sourceKey);
        result.SqliteKey.Should().Be("views/cphs_20260623T120000Z.sqlite");
        result.RowCount.Should().Be(2);

        _internalStorageMock.Verify(
            s => s.UploadAsync(
                "views/cphs_20260623T120000Z.sqlite",
                It.IsAny<byte[]>(),
                "application/x-sqlite3",
                null,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ExportAsync_WhenSqliteAlreadyExists_SkipsUpload()
    {
        var duckDbPath = CreateTestDuckDb(["01/001/0001"]);
        var duckDbBytes = await File.ReadAllBytesAsync(duckDbPath);
        var sourceKey = "staging/keeper_data_bridge_20260623T120000Z.duckdb";

        _internalStorageMock
            .Setup(s => s.OpenReadAsync(sourceKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new MemoryStream(duckDbBytes));

        _internalStorageMock
            .Setup(s => s.ExistsAsync("views/cphs_20260623T120000Z.sqlite", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var result = await _sut.ExportAsync(sourceKey);

        result.RowCount.Should().Be(1);

        _internalStorageMock.Verify(
            s => s.UploadAsync(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<string>(), It.IsAny<IReadOnlyDictionary<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ExportAsync_NoSourceKey_FindsLatestAndExports()
    {
        var duckDbPath = CreateTestDuckDb(["10/100/1000"]);
        var duckDbBytes = await File.ReadAllBytesAsync(duckDbPath);

        var objects = new List<StorageObjectInfo>
        {
            new() { Key = "staging/keeper_data_bridge_20260620T100000Z.duckdb", Container = "bucket", Size = 100, LastModified = DateTimeOffset.UtcNow.AddDays(-3), StorageUri = new Uri("s3://bucket/staging/keeper_data_bridge_20260620T100000Z.duckdb") },
            new() { Key = "staging/keeper_data_bridge_20260623T120000Z.duckdb", Container = "bucket", Size = 200, LastModified = DateTimeOffset.UtcNow, StorageUri = new Uri("s3://bucket/staging/keeper_data_bridge_20260623T120000Z.duckdb") }
        };

        _internalStorageMock
            .Setup(s => s.ListAsync("staging/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(objects);

        _internalStorageMock
            .Setup(s => s.OpenReadAsync("staging/keeper_data_bridge_20260623T120000Z.duckdb", It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new MemoryStream(duckDbBytes));

        _internalStorageMock
            .Setup(s => s.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var result = await _sut.ExportAsync();

        result.SourceDuckDbKey.Should().Be("staging/keeper_data_bridge_20260623T120000Z.duckdb");
        result.SqliteKey.Should().Be("views/cphs_20260623T120000Z.sqlite");
        result.RowCount.Should().Be(1);
    }

    [Fact]
    public async Task ExportAsync_NoDuckDbFiles_ThrowsPipelineExecutionException()
    {
        _internalStorageMock
            .Setup(s => s.ListAsync("staging/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<StorageObjectInfo>());

        var act = async () => await _sut.ExportAsync();

        await act.Should().ThrowAsync<PipelineExecutionException>()
            .WithInnerException<PipelineExecutionException, InvalidOperationException>()
            .WithMessage("*No DuckDB staging files found*");
    }

    private string CreateTestDuckDb(string?[] cphValues)
    {
        var duckDbPath = Path.Combine(_tempDir, $"{Guid.NewGuid()}.duckdb");

        using var connection = new DuckDBConnection($"Data Source={duckDbPath}");
        connection.Open();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = "CREATE TABLE sam_cph_holdings (BATCH_ID VARCHAR, CHANGE_TYPE VARCHAR, CPH VARCHAR, FEATURE_NAME VARCHAR)";
        createCmd.ExecuteNonQuery();

        foreach (var cph in cphValues)
        {
            using var insertCmd = connection.CreateCommand();
            insertCmd.CommandText = cph is null
                ? "INSERT INTO sam_cph_holdings (BATCH_ID, CHANGE_TYPE, CPH, FEATURE_NAME) VALUES ('1', 'I', NULL, 'test')"
                : $"INSERT INTO sam_cph_holdings (BATCH_ID, CHANGE_TYPE, CPH, FEATURE_NAME) VALUES ('1', 'I', '{cph}', 'test')";
            insertCmd.ExecuteNonQuery();
        }

        return duckDbPath;
    }
}
