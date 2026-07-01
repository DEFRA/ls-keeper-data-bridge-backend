using DuckDB.NET.Data;
using FluentAssertions;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using KeeperData.Infrastructure.ETL;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.ETL;

public class CphExportServiceTests : IDisposable
{
    private readonly Mock<IBlobStorageServiceFactory> _storageFactoryMock;
    private readonly Mock<IBlobStorageService> _internalStorageMock;
    private readonly Mock<ILogger<CphExportService>> _loggerMock;
    private readonly CphExportService _sut;
    private readonly string _tempDir;

    public CphExportServiceTests()
    {
        _storageFactoryMock = new Mock<IBlobStorageServiceFactory>();
        _internalStorageMock = new Mock<IBlobStorageService>();
        _loggerMock = new Mock<ILogger<CphExportService>>();

        _storageFactoryMock
            .Setup(f => f.GetSourceInternal())
            .Returns(_internalStorageMock.Object);

        _sut = new CphExportService(_storageFactoryMock.Object, _loggerMock.Object);
        _tempDir = Path.Combine(Path.GetTempPath(), "cph-export-tests", Guid.NewGuid().ToString());
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
    public void ReadDistinctCphs_ExtractsDistinctNonNullCphs()
    {
        var duckDbPath = CreateTestDuckDb(["01/001/0001", "02/002/0002", "01/001/0001", "", null]);

        var result = CphExportService.ReadDistinctCphs(duckDbPath);

        result.Should().BeEquivalentTo(["01/001/0001", "02/002/0002"]);
    }

    [Fact]
    public void ReadDistinctCphs_ReturnsOrderedResults()
    {
        var duckDbPath = CreateTestDuckDb(["99/999/9999", "01/001/0001", "50/500/5000"]);

        var result = CphExportService.ReadDistinctCphs(duckDbPath);

        result.Should().BeInAscendingOrder();
        result.Should().HaveCount(3);
    }

    [Fact]
    public void ReadDistinctCphs_EmptyTable_ReturnsEmptyList()
    {
        var duckDbPath = CreateTestDuckDb([]);

        var result = CphExportService.ReadDistinctCphs(duckDbPath);

        result.Should().BeEmpty();
    }

    [Fact]
    public void ReadDistinctCphs_AllNullOrEmpty_ReturnsEmptyList()
    {
        var duckDbPath = CreateTestDuckDb([null, "", null, ""]);

        var result = CphExportService.ReadDistinctCphs(duckDbPath);

        result.Should().BeEmpty();
    }

    [Fact]
    public void WriteSqlite_CreatesTableWithCorrectData()
    {
        var sqlitePath = Path.Combine(_tempDir, "test.sqlite");
        var cphs = new List<string> { "01/001/0001", "02/002/0002", "03/003/0003" };

        CphExportService.WriteSqlite(sqlitePath, cphs);

        using var connection = new SqliteConnection($"Data Source={sqlitePath};Mode=ReadOnly");
        connection.Open();

        using var countCmd = connection.CreateCommand();
        countCmd.CommandText = "SELECT COUNT(*) FROM cphs";
        var count = (long)countCmd.ExecuteScalar()!;
        count.Should().Be(3);

        using var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT CPH FROM cphs ORDER BY CPH";
        var reader = selectCmd.ExecuteReader();
        var readCphs = new List<string>();
        while (reader.Read())
        {
            readCphs.Add(reader.GetString(0));
        }

        readCphs.Should().BeEquivalentTo(cphs);
    }

    [Fact]
    public void WriteSqlite_EmptyList_CreatesEmptyTable()
    {
        var sqlitePath = Path.Combine(_tempDir, "empty.sqlite");

        CphExportService.WriteSqlite(sqlitePath, []);

        using var connection = new SqliteConnection($"Data Source={sqlitePath};Mode=ReadOnly");
        connection.Open();

        using var countCmd = connection.CreateCommand();
        countCmd.CommandText = "SELECT COUNT(*) FROM cphs";
        var count = (long)countCmd.ExecuteScalar()!;
        count.Should().Be(0);
    }

    [Fact]
    public async Task ExportAsync_WithSourceKey_DownloadsProcessesAndUploads()
    {
        var duckDbPath = CreateTestDuckDb(["01/001/0001", "02/002/0002", "01/001/0001"]);
        var duckDbBytes = await File.ReadAllBytesAsync(duckDbPath);
        var sourceKey = "staging/keeper_data_bridge_20260623T120000Z.duckdb";

        _internalStorageMock
            .Setup(s => s.OpenReadAsync(sourceKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MemoryStream(duckDbBytes));

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
            .ReturnsAsync(new MemoryStream(duckDbBytes));

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
            .ReturnsAsync(new MemoryStream(duckDbBytes));

        _internalStorageMock
            .Setup(s => s.ExistsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var result = await _sut.ExportAsync();

        result.SourceDuckDbKey.Should().Be("staging/keeper_data_bridge_20260623T120000Z.duckdb");
        result.SqliteKey.Should().Be("views/cphs_20260623T120000Z.sqlite");
        result.RowCount.Should().Be(1);
    }

    [Fact]
    public async Task ExportAsync_NoDuckDbFiles_ThrowsInvalidOperationException()
    {
        _internalStorageMock
            .Setup(s => s.ListAsync("staging/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<StorageObjectInfo>());

        var act = async () => await _sut.ExportAsync();

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*No DuckDB staging files found*");
    }

    private string CreateTestDuckDb(string?[] cphValues)
    {
        var duckDbPath = Path.Combine(_tempDir, $"{Guid.NewGuid()}.duckdb");

        using var connection = new DuckDBConnection($"Data Source={duckDbPath}");
        connection.Open();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = """
            CREATE TABLE sam_cph_holdings (
                BATCH_ID VARCHAR,
                CHANGE_TYPE VARCHAR,
                CPH VARCHAR,
                FEATURE_NAME VARCHAR,
                CPH_TYPE VARCHAR,
                ADDRESS_PK VARCHAR,
                SAON_START_NUMBER VARCHAR,
                SAON_START_NUMBER_SUFFIX VARCHAR,
                SAON_END_NUMBER VARCHAR,
                SAON_END_NUMBER_SUFFIX VARCHAR,
                PAON_START_NUMBER VARCHAR,
                PAON_START_NUMBER_SUFFIX VARCHAR,
                PAON_END_NUMBER VARCHAR,
                PAON_END_NUMBER_SUFFIX VARCHAR,
                STREET VARCHAR,
                TOWN VARCHAR,
                LOCALITY VARCHAR,
                UK_INTERNAL_CODE VARCHAR,
                POSTCODE VARCHAR,
                COUNTRY_CODE VARCHAR,
                UDPRN VARCHAR,
                EASTING VARCHAR,
                NORTHING VARCHAR,
                OS_MAP_REFERENCE VARCHAR,
                DISEASE_TYPE VARCHAR,
                INTERVAL VARCHAR,
                INTERVAL_UNIT_OF_TIME VARCHAR,
                FEATURE_ADDRESS_FROM_DATE VARCHAR,
                FEATURE_ADDRESS_TO_DATE VARCHAR,
                CPH_RELATIONSHIP_TYPE VARCHAR,
                SECONDARY_CPH VARCHAR,
                FACILITY_BUSINSS_ACTVTY_CODE VARCHAR,
                FACILITY_TYPE_CODE VARCHAR,
                FCLTY_SUB_BSNSS_ACTVTY_CODE VARCHAR,
                FEATURE_STATUS_CODE VARCHAR,
                MOVEMENT_RSTRCTN_RSN_CODE VARCHAR,
                ANIMAL_SPECIES_CODE VARCHAR,
                ANIMAL_PRODUCTION_USAGE_CODE VARCHAR
            )
            """;
        createCmd.ExecuteNonQuery();

        foreach (var cph in cphValues)
        {
            using var insertCmd = connection.CreateCommand();
            if (cph is null)
            {
                insertCmd.CommandText = """
                    INSERT INTO sam_cph_holdings (BATCH_ID, CHANGE_TYPE, CPH, FEATURE_NAME)
                    VALUES ('1', 'I', NULL, 'test')
                    """;
            }
            else
            {
                insertCmd.CommandText = $"""
                    INSERT INTO sam_cph_holdings (BATCH_ID, CHANGE_TYPE, CPH, FEATURE_NAME)
                    VALUES ('1', 'I', '{cph}', 'test')
                    """;
            }

            insertCmd.ExecuteNonQuery();
        }

        return duckDbPath;
    }
}
