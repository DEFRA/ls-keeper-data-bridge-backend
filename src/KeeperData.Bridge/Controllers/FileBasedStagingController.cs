using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Extensions;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

/// <summary>Download access to the DuckDB staging databases the load stage produces.
/// A prototype stand-in for the presigned-URI API, in the same shape as the SQLite endpoint.</summary>
[ApiController]
[Route("api/etl/file-based/staging")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class FileBasedStagingController(
    IBlobStorageServiceFactory blobStorageServiceFactory,
    ILogger<FileBasedStagingController> logger) : ControllerBase
{
    private const string StagingPrefix = $"{EtlPipelineFolders.Staging}/";
    private static readonly TimeSpan DefaultPresignedUrlExpiry = TimeSpan.FromHours(1);

    /// <summary>
    /// Returns a presigned download URL for the latest DuckDB staging database.
    /// The URL is valid for 1 hour by default.
    /// </summary>
    /// <param name="expiresInMinutes">Optional expiry in minutes (default: 60, max: 10080 / 7 days)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    [HttpGet("duckdb/latest")]
    [ProducesResponseType(typeof(StagingDatabaseLatestResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(StagingDatabaseErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(StagingDatabaseErrorResponse), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> GetLatestDuckDbUrl(
        [FromQuery] int? expiresInMinutes = null,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request for latest DuckDB staging presigned URL");

        try
        {
            var storageService = blobStorageServiceFactory.GetSourceInternal();
            var objects = await storageService.ListAsync(StagingPrefix, cancellationToken);

            var latest = objects.GetLatest();

            if (latest is null) return ObjectNotFound;

            var expiry = expiresInMinutes.HasValue
                ? TimeSpan.FromMinutes(Math.Clamp(expiresInMinutes.Value, 1, 10080))
                : DefaultPresignedUrlExpiry;

            var presignedUrl = storageService.GeneratePresignedUrl(latest.Key, expiry);

            logger.LogInformation("Generated presigned URL for {Key} (expires in {ExpiryMinutes} min)",
                latest.Key, expiry.TotalMinutes);

            return PresignedUrlReady(latest, presignedUrl, expiry);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get latest DuckDB staging request was cancelled");
            return RequestCancelled();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error getting latest DuckDB staging presigned URL");
            return PresignedUrlFailed();
        }
    }

    private OkObjectResult PresignedUrlReady(StorageObjectInfo latest, string presignedUrl, TimeSpan expiry)
        => Ok(new StagingDatabaseLatestResponse
        {
            ObjectKey = latest.Key,
            DownloadUrl = presignedUrl,
            Size = latest.Size,
            LastModified = latest.LastModified,
            ExpiresAt = DateTime.UtcNow.Add(expiry)
        });

    private ObjectResult RequestCancelled()
        => StatusCode(499, new StagingDatabaseErrorResponse
        {
            Message = "Request was cancelled.",
            Timestamp = DateTime.UtcNow
        });

    private ObjectResult PresignedUrlFailed()
        => StatusCode(StatusCodes.Status500InternalServerError, new StagingDatabaseErrorResponse
        {
            Message = "An unexpected error occurred while generating the download URL.",
            Timestamp = DateTime.UtcNow
        });

    private NotFoundObjectResult ObjectNotFound
    {
        get
        {
            logger.LogWarning("No DuckDB staging files found in {Prefix}", StagingPrefix);
            return NotFound(new StagingDatabaseErrorResponse
            {
                Message = "No DuckDB staging databases found. Run the file-based ETL pipeline first.",
                Timestamp = DateTime.UtcNow
            });
        }
    }
}

#region Response DTOs

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record StagingDatabaseLatestResponse
{
    public required string ObjectKey { get; init; }
    public required string DownloadUrl { get; init; }
    public long Size { get; init; }
    public DateTimeOffset LastModified { get; init; }
    public DateTime ExpiresAt { get; init; }
}

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record StagingDatabaseErrorResponse
{
    public required string Message { get; init; }
    public DateTime Timestamp { get; init; }
}

#endregion
