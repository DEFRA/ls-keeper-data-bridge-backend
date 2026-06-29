using Amazon.S3;
using Amazon.S3.Model;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

/// <summary>
/// Fixture that generates a DuckDB stub file locally, mirroring the Phase I staging output.
/// Call <see cref="UploadToS3Async"/> with a live S3 client to place it at staging/keeper_data_bridge_*.duckdb.
/// </summary>
public class DuckDbStubFixture : IAsyncLifetime
{
    private string _tempDir = null!;

    public string LocalFilePath { get; private set; } = null!;
    public string StagingKey { get; private set; } = null!;
    public DateTime StubTimestamp { get; } = new(2026, 6, 23, 12, 0, 0, DateTimeKind.Utc);
    public int RowCount { get; } = 750;

    public Task InitializeAsync()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"duckdb_stub_{Guid.NewGuid():N}");
        LocalFilePath = DuckDbStubGenerator.BuildStagingPath(_tempDir, StubTimestamp);

        DuckDbStubGenerator.Generate(LocalFilePath, RowCount);

        StagingKey = $"staging/keeper_data_bridge_{StubTimestamp:yyyyMMdd'T'HHmmss'Z'}.duckdb";

        return Task.CompletedTask;
    }

    public async Task UploadToS3Async(IAmazonS3 s3Client, string bucket)
    {
        await using var fileStream = File.OpenRead(LocalFilePath);
        await s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucket,
            Key = StagingKey,
            InputStream = fileStream,
            ContentType = "application/octet-stream",
            ServerSideEncryptionMethod = ServerSideEncryptionMethod.None
        });
    }

    public async Task<Stream> DownloadFromS3Async(IAmazonS3 s3Client, string bucket)
    {
        var response = await s3Client.GetObjectAsync(bucket, StagingKey);
        return response.ResponseStream;
    }

    public Task DisposeAsync()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
        return Task.CompletedTask;
    }
}
