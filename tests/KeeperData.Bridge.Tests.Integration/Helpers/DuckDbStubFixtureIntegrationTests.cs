using System.Text.RegularExpressions;
using Amazon.S3.Model;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

/// <summary>
/// Integration tests verifying the DuckDB stub fixture uploads to LocalStack S3
/// and can be downloaded and queried — the same flow Phase II export code will use.
/// </summary>
[Collection("LocalStackAndDuckDb"), Trait("Dependence", "docker")]
public partial class DuckDbStubFixtureIntegrationTests : IAsyncLifetime
{
    [GeneratedRegex(@"^\d{2}/\d{3}/\d{4}$")]
    private static partial Regex CphFormatRegex();
    private readonly ITestOutputHelper _output;
    private readonly LocalStackFixture _localStackFixture;
    private readonly DuckDbStubFixture _duckDbStubFixture;

    public DuckDbStubFixtureIntegrationTests(
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
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task StubDuckDb_ExistsInS3AtStagingPath()
    {
        var response = await _localStackFixture.S3Client.GetObjectMetadataAsync(
            LocalStackFixture.TestBucket, _duckDbStubFixture.StagingKey);

        response.ContentLength.Should().BeGreaterThan(0);
        _output.WriteLine($"DuckDB stub at s3://{LocalStackFixture.TestBucket}/{_duckDbStubFixture.StagingKey} ({response.ContentLength} bytes)");
    }

    [Fact]
    public async Task StubDuckDb_CanBeDownloadedAndQueried()
    {
        var tempPath = Path.Combine(Path.GetTempPath(), $"duckdb_s3_test_{Guid.NewGuid():N}.duckdb");
        try
        {
            await using (var s3Stream = await _duckDbStubFixture.DownloadFromS3Async(
                _localStackFixture.S3Client, LocalStackFixture.TestBucket))
            await using (var fileStream = File.Create(tempPath))
            {
                await s3Stream.CopyToAsync(fileStream);
            }

            using var conn = new DuckDBConnection($"Data Source={tempPath}");
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM sam_cph_holdings";
            var count = (long)cmd.ExecuteScalar()!;

            count.Should().Be(_duckDbStubFixture.RowCount);
            _output.WriteLine($"Downloaded DuckDB from S3 and queried {count} rows successfully");
        }
        finally
        {
            if (File.Exists(tempPath))
                File.Delete(tempPath);
        }
    }

    [Fact]
    public async Task StubDuckDb_DistinctCphQueryWorksAfterS3RoundTrip()
    {
        var tempPath = Path.Combine(Path.GetTempPath(), $"duckdb_s3_test_{Guid.NewGuid():N}.duckdb");
        try
        {
            await using (var s3Stream = await _duckDbStubFixture.DownloadFromS3Async(
                _localStackFixture.S3Client, LocalStackFixture.TestBucket))
            await using (var fileStream = File.Create(tempPath))
            {
                await s3Stream.CopyToAsync(fileStream);
            }

            using var conn = new DuckDBConnection($"Data Source={tempPath}");
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT DISTINCT CPH FROM sam_cph_holdings WHERE CPH IS NOT NULL AND CPH <> '' ORDER BY CPH";
            using var reader = cmd.ExecuteReader();

            var cphs = new List<string>();
            while (reader.Read())
                cphs.Add(reader.GetString(0));

            cphs.Should().NotBeEmpty();
            cphs.Should().OnlyContain(c => CphFormatRegex().IsMatch(c));
            _output.WriteLine($"Found {cphs.Count} distinct CPHs after S3 round-trip");
        }
        finally
        {
            if (File.Exists(tempPath))
                File.Delete(tempPath);
        }
    }

    [Fact]
    public async Task StubDuckDb_StagingKeyFollowsPhaseINamingConvention()
    {
        _duckDbStubFixture.StagingKey.Should().MatchRegex(
            @"^staging/keeper_data_bridge_\d{8}T\d{6}Z\.duckdb$");

        var listed = await _localStackFixture.S3Client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = LocalStackFixture.TestBucket,
            Prefix = "staging/"
        });

        listed.S3Objects.Should().Contain(o => o.Key == _duckDbStubFixture.StagingKey);
        _output.WriteLine($"Staging key: {_duckDbStubFixture.StagingKey}");
    }
}
