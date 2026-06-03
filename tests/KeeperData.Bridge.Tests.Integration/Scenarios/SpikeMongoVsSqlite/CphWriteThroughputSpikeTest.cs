using System.Diagnostics;
using System.IO;
using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using Microsoft.Data.Sqlite;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Scenarios.SpikeMongoVsSqlite;

[Collection("MongoDB"), Trait("Dependence", "docker")]
public class CphWriteThroughputSpikeTest : IAsyncLifetime
{
    private const int TargetRowCount = 10_000;
    private const int ReadLookupCount = 10000;
    private const int ReadWarmupCount = 10;
    private const string CsvSourceFolder = @"C:\temp\spike-csv";
    private const string CsvSourcePattern = "LITP_SAMCPHHOLDING_*.csv";

    private const string MongoDatabaseName = "spike-cph-throughput";
    private const string MongoCollectionName = "cph_spike";
    private const string PrimaryKeyField = "CPH";
    private const string ChangeTypeField = "CHANGE_TYPE";

    private readonly MongoDbFixture _mongo;
    private readonly ITestOutputHelper _output;
    private readonly List<string> _sqliteFilesCreated = new();

    public CphWriteThroughputSpikeTest(MongoDbFixture mongo, ITestOutputHelper output)
    {
        _mongo = mongo;
        _output = output;
    }

    public async Task InitializeAsync()
    {
        await _mongo.MongoClient.DropDatabaseAsync(MongoDatabaseName);
    }

    public async Task DisposeAsync()
    {
        try
        {
            await _mongo.MongoClient.DropDatabaseAsync(MongoDatabaseName);
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Cleanup warning: could not drop Mongo db '{MongoDatabaseName}': {ex.GetType().Name}: {ex.Message}");
        }

        foreach (var path in _sqliteFilesCreated)
        {
            TryDeleteSqliteFile(path);
        }
    }

    [Fact]
    public async Task SQLite_Vs_Mongo_BulkInsert_10000_CphRows()
    {
        var rows = LoadOrSynthesizeRows();
        _output.WriteLine($"Row source: {rows.SourceDescription}");
        _output.WriteLine($"Rows ready: {rows.Rows.Count}");
        _output.WriteLine("");

        var sqliteResult = await PopulateSqliteAsync(rows.Rows);
        var mongoResult = await PopulateMongoAsync(rows.Rows);

        LogWriteComparison(rows.Rows.Count, sqliteResult.WriteMs, mongoResult.WriteMs);
    }

    [Fact]
    public async Task SQLite_Vs_Mongo_EqualityRead_100_CphLookups()
    {
        var rows = LoadOrSynthesizeRows();
        _output.WriteLine($"Row source: {rows.SourceDescription}");
        _output.WriteLine($"Rows ready: {rows.Rows.Count}");
        _output.WriteLine("Populating both stores (untimed)...");
        _output.WriteLine("");

        var sqliteResult = await PopulateSqliteAsync(rows.Rows);
        var mongoResult = await PopulateMongoAsync(rows.Rows);

        var lookupKeys = SelectLookupKeys(rows.Rows, ReadLookupCount);

        var sqliteReadMs = await TimeSqliteEqualityReadsAsync(sqliteResult.DbPath, lookupKeys);
        var mongoReadMs = await TimeMongoEqualityReadsAsync(mongoResult.Collection, lookupKeys);

        LogReadComparison(rows.Rows.Count, lookupKeys.Count, sqliteReadMs, mongoReadMs);
    }

    private async Task<SqlitePopulateResult> PopulateSqliteAsync(IReadOnlyList<Dictionary<string, string>> rows)
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"spike-cph-{Guid.NewGuid():N}.db");
        if (File.Exists(dbPath)) File.Delete(dbPath);
        _sqliteFilesCreated.Add(dbPath);

        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Pooling = false
        }.ToString();

        await using var conn = new SqliteConnection(connectionString);
        await conn.OpenAsync();

        await using (var ddl = conn.CreateCommand())
        {
            ddl.CommandText = """
                CREATE TABLE IF NOT EXISTS cph_spike (
                    CPH         TEXT PRIMARY KEY,
                    FarmName    TEXT,
                    Owner       TEXT,
                    Address     TEXT,
                    CHANGE_TYPE TEXT
                );
                """;
            await ddl.ExecuteNonQueryAsync();
        }

        await using var tx = (SqliteTransaction)await conn.BeginTransactionAsync();

        await using var upsert = conn.CreateCommand();
        upsert.Transaction = tx;
        upsert.CommandText = """
            INSERT INTO cph_spike (CPH, FarmName, Owner, Address, CHANGE_TYPE)
            VALUES (@cph, @farm, @owner, @addr, @change)
            ON CONFLICT(CPH) DO UPDATE SET
                FarmName    = excluded.FarmName,
                Owner       = excluded.Owner,
                Address     = excluded.Address,
                CHANGE_TYPE = excluded.CHANGE_TYPE;
            """;
        var pCph = upsert.Parameters.Add("@cph", SqliteType.Text);
        var pFarm = upsert.Parameters.Add("@farm", SqliteType.Text);
        var pOwner = upsert.Parameters.Add("@owner", SqliteType.Text);
        var pAddr = upsert.Parameters.Add("@addr", SqliteType.Text);
        var pChange = upsert.Parameters.Add("@change", SqliteType.Text);

        var sw = Stopwatch.StartNew();
        foreach (var row in rows)
        {
            pCph.Value = row.GetValueOrDefault("CPH", string.Empty);
            pFarm.Value = row.GetValueOrDefault("FarmName", string.Empty);
            pOwner.Value = row.GetValueOrDefault("Owner", string.Empty);
            pAddr.Value = row.GetValueOrDefault("Address", string.Empty);
            pChange.Value = row.GetValueOrDefault("CHANGE_TYPE", "I");
            await upsert.ExecuteNonQueryAsync();
        }
        await tx.CommitAsync();
        sw.Stop();

        await using var countCmd = conn.CreateCommand();
        countCmd.CommandText = "SELECT COUNT(*) FROM cph_spike";
        var count = (long)(await countCmd.ExecuteScalarAsync() ?? 0L);
        count.Should().Be(rows.Count);

        return new SqlitePopulateResult(dbPath, sw.ElapsedMilliseconds);
    }

    private async Task<MongoPopulateResult> PopulateMongoAsync(IReadOnlyList<Dictionary<string, string>> rows)
    {
        var database = _mongo.MongoClient.GetDatabase(MongoDatabaseName);
        await database.DropCollectionAsync(MongoCollectionName);
        var collection = database.GetCollection<BsonDocument>(MongoCollectionName);

        var wildcardKeys = Builders<BsonDocument>.IndexKeys.Wildcard();
        await collection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(wildcardKeys));

        var bulkOps = BuildBulkOps(rows);

        var sw = Stopwatch.StartNew();
        await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
        sw.Stop();

        var storedCount = await collection.CountDocumentsAsync(Builders<BsonDocument>.Filter.Empty);
        storedCount.Should().Be(rows.Count);

        return new MongoPopulateResult(collection, sw.ElapsedMilliseconds);
    }

    private async Task<long> TimeSqliteEqualityReadsAsync(string dbPath, IReadOnlyList<string> lookupKeys)
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadOnly,
            Pooling = false
        }.ToString();

        await using var conn = new SqliteConnection(connectionString);
        await conn.OpenAsync();

        await using var select = conn.CreateCommand();
        select.CommandText = "SELECT CPH, FarmName, Owner, Address, CHANGE_TYPE FROM cph_spike WHERE CPH = @cph LIMIT 1;";
        var pCph = select.Parameters.Add("@cph", SqliteType.Text);

        for (var i = 0; i < ReadWarmupCount && i < lookupKeys.Count; i++)
        {
            pCph.Value = lookupKeys[i];
            await using var warmReader = await select.ExecuteReaderAsync();
            while (await warmReader.ReadAsync()) { }
        }

        var hits = 0;
        var sw = Stopwatch.StartNew();
        foreach (var key in lookupKeys)
        {
            pCph.Value = key;
            await using var reader = await select.ExecuteReaderAsync();
            if (await reader.ReadAsync()) hits++;
        }
        sw.Stop();

        hits.Should().Be(lookupKeys.Count, "every lookup key was picked from the populated rows");
        return sw.ElapsedMilliseconds;
    }

    private static async Task<long> TimeMongoEqualityReadsAsync(IMongoCollection<BsonDocument> collection, IReadOnlyList<string> lookupKeys)
    {
        var filterBuilder = Builders<BsonDocument>.Filter;

        for (var i = 0; i < ReadWarmupCount && i < lookupKeys.Count; i++)
        {
            await collection.Find(filterBuilder.Eq(PrimaryKeyField, lookupKeys[i])).FirstOrDefaultAsync();
        }

        var hits = 0;
        var sw = Stopwatch.StartNew();
        foreach (var key in lookupKeys)
        {
            var doc = await collection.Find(filterBuilder.Eq(PrimaryKeyField, key)).FirstOrDefaultAsync();
            if (doc is not null) hits++;
        }
        sw.Stop();

        hits.Should().Be(lookupKeys.Count, "every lookup key was picked from the populated rows");
        return sw.ElapsedMilliseconds;
    }

    private static List<string> SelectLookupKeys(IReadOnlyList<Dictionary<string, string>> rows, int count)
    {
        if (rows.Count == 0) return new List<string>();

        var rng = new Random(7);
        var keys = new HashSet<string>(StringComparer.Ordinal);
        var safetyLimit = Math.Min(count * 10, rows.Count);
        var attempts = 0;

        while (keys.Count < count && attempts < safetyLimit)
        {
            var candidate = rows[rng.Next(rows.Count)].GetValueOrDefault(PrimaryKeyField);
            if (!string.IsNullOrEmpty(candidate))
            {
                keys.Add(candidate);
            }
            attempts++;
        }

        return keys.ToList();
    }

    private void TryDeleteSqliteFile(string path)
    {
        if (!File.Exists(path)) return;
        try
        {
            File.Delete(path);
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Cleanup warning: could not delete '{path}': {ex.GetType().Name}: {ex.Message}");
        }
    }

    private static List<WriteModel<BsonDocument>> BuildBulkOps(IReadOnlyList<Dictionary<string, string>> rows)
    {
        var ops = new List<WriteModel<BsonDocument>>(rows.Count);
        var filterBuilder = Builders<BsonDocument>.Filter;

        foreach (var row in rows)
        {
            var cph = row.GetValueOrDefault(PrimaryKeyField);
            if (string.IsNullOrEmpty(cph)) continue;

            var changeType = row.GetValueOrDefault(ChangeTypeField, "I");
            var filter = filterBuilder.Eq(PrimaryKeyField, cph);

            if (string.Equals(changeType, "D", StringComparison.OrdinalIgnoreCase))
            {
                ops.Add(new DeleteOneModel<BsonDocument>(filter));
            }
            else
            {
                var update = Builders<BsonDocument>.Update.Combine(
                    row.Select(kvp => Builders<BsonDocument>.Update.Set(kvp.Key, kvp.Value)));
                ops.Add(new UpdateOneModel<BsonDocument>(filter, update) { IsUpsert = true });
            }
        }

        return ops;
    }

    private RowSource LoadOrSynthesizeRows()
    {
        var sourceFile = FindCphCsvFile();
        if (sourceFile is not null)
        {
            var loaded = LoadCsv(sourceFile);
            return new RowSource(loaded, $"loaded from {sourceFile} ({loaded.Count} rows)");
        }

        var synthesized = GenerateSyntheticRows(TargetRowCount);
        return new RowSource(synthesized, $"synthesized {synthesized.Count} rows (no CPH csv found at {CsvSourceFolder})");
    }

    private static string? FindCphCsvFile()
    {
        if (!Directory.Exists(CsvSourceFolder)) return null;
        return Directory.EnumerateFiles(CsvSourceFolder, CsvSourcePattern).FirstOrDefault();
    }

    private static List<Dictionary<string, string>> LoadCsv(string path)
    {
        var rows = new List<Dictionary<string, string>>();
        using var reader = new StreamReader(path);

        var headerLine = reader.ReadLine();
        if (headerLine is null) return rows;
        var headers = headerLine.Split('|');

        string? line;
        while ((line = reader.ReadLine()) is not null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            var values = line.Split('|');
            var row = new Dictionary<string, string>(headers.Length, StringComparer.Ordinal);
            for (var i = 0; i < headers.Length && i < values.Length; i++)
            {
                row[headers[i]] = values[i];
            }
            rows.Add(row);
        }
        return rows;
    }

    private static List<Dictionary<string, string>> GenerateSyntheticRows(int count)
    {
        var rng = new Random(42);
        var rows = new List<Dictionary<string, string>>(count);
        for (var i = 0; i < count; i++)
        {
            rows.Add(new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["CPH"] = $"CPH{i:D8}",
                ["FarmName"] = $"Farm {i}",
                ["Owner"] = $"Owner {rng.Next(1, 5000)}",
                ["Address"] = $"Address line {i}",
                ["CHANGE_TYPE"] = "I"
            });
        }
        return rows;
    }

    private void LogWriteComparison(int rowCount, long sqliteMs, long mongoMs)
    {
        var sqliteHuman = Humanize(sqliteMs);
        var mongoHuman = Humanize(mongoMs);
        var verdict = FormatVerdict(sqliteMs, mongoMs);

        _output.WriteLine($"=== CPH Write Throughput Spike ({rowCount} rows) ===");
        _output.WriteLine($"SQLite: {sqliteHuman} (in-process, PK index only, single transaction)");
        _output.WriteLine($"Mongo:  {mongoHuman} (testcontainers TCP, $** wildcard index, BulkWriteAsync IsOrdered=false)");
        _output.WriteLine($"Verdict: {verdict}");
    }

    private void LogReadComparison(int rowCount, int lookupCount, long sqliteMs, long mongoMs)
    {
        var sqliteHuman = Humanize(sqliteMs);
        var mongoHuman = Humanize(mongoMs);
        var sqliteAvg = lookupCount == 0 ? 0 : (double)sqliteMs / lookupCount;
        var mongoAvg = lookupCount == 0 ? 0 : (double)mongoMs / lookupCount;
        var verdict = FormatVerdict(sqliteMs, mongoMs);

        _output.WriteLine($"=== CPH Equality Read Spike ({rowCount} rows populated, {lookupCount} lookups, after {ReadWarmupCount} warmup) ===");
        _output.WriteLine($"SQLite: {sqliteHuman} total, avg {sqliteAvg:F2}ms/lookup (in-process, PK index, prepared statement)");
        _output.WriteLine($"Mongo:  {mongoHuman} total, avg {mongoAvg:F2}ms/lookup (testcontainers TCP, $** wildcard index, bare driver Find)");
        _output.WriteLine($"Verdict: {verdict}");
    }

    private static string FormatVerdict(long sqliteMs, long mongoMs)
    {
        if (sqliteMs == 0 && mongoMs == 0) return "Both completed in under 1ms; ratio therefore is not meaningful.";
        if (sqliteMs == 0) return $"Mongo {mongoMs}ms vs SQLite <1ms; SQLite faster (ratio not computed).";
        if (mongoMs == 0) return $"SQLite {sqliteMs}ms vs Mongo <1ms; Mongo faster (ratio not computed).";

        if (mongoMs > sqliteMs)
        {
            var ratio = (double)mongoMs / sqliteMs;
            return $"Mongo is {ratio:F1}x slower than SQLite";
        }
        else
        {
            var ratio = (double)sqliteMs / mongoMs;
            return $"SQLite is {ratio:F1}x slower than Mongo";
        }
    }

    private static string Humanize(long milliseconds)
    {
        if (milliseconds < 1000) return $"{milliseconds}ms";
        var totalSeconds = milliseconds / 1000.0;
        if (totalSeconds < 60) return $"{totalSeconds:0.00}s";
        var minutes = (int)(totalSeconds / 60);
        var seconds = totalSeconds - (minutes * 60);
        return $"{minutes}min {seconds:0}secs";
    }

    private sealed record RowSource(List<Dictionary<string, string>> Rows, string SourceDescription);
    private sealed record SqlitePopulateResult(string DbPath, long WriteMs);
    private sealed record MongoPopulateResult(IMongoCollection<BsonDocument> Collection, long WriteMs);
}
