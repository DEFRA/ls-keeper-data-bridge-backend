using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Extensions;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

/// <summary>Download access to the DuckDB staging database and SQLite read model produced by the
/// ETL pipeline.</summary>
[ApiController]
[Route("api/etl/staging")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class EtlStagingController(
    IEtlPipelineStorageProvider storageProvider,
    ILogger<EtlStagingController> logger) : ControllerBase
{
    private static readonly TimeSpan DefaultPresignedUrlExpiry = TimeSpan.FromHours(1);

    private const int MaxSqlitePresignedUrlMinutes = 60;

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
            // The pipeline folders sit at the bucket root, not under the legacy source prefix, so
            // this has to read them through the same provider the load stage writes with.
            var storageService = storageProvider.ForFolder(EtlPipelineFolders.Staging);
            var objects = await storageService.ListAsync(string.Empty, cancellationToken);

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

    /// <summary>
    /// Returns a presigned download URL for the latest SQLite read model.
    /// </summary>
    /// <param name="expiresInMinutes">Optional expiry in minutes (default: 60, max: 60)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    [HttpGet("sqlite/latest")]
    [ProducesResponseType(typeof(StagingDatabaseLatestResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(StagingDatabaseErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(StagingDatabaseErrorResponse), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> GetLatestSqliteUrl(
        [FromQuery] int? expiresInMinutes = null,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request for latest SQLite read model presigned URL");

        try
        {
            var storageService = storageProvider.ForFolder(EtlPipelineFolders.Views);
            var objects = await storageService.ListAsync(string.Empty, cancellationToken);

            var latest = objects.GetLatestSqliteView();

            if (latest is null) return SqliteNotFound;

            // The read model carries keeper names, emails and telephone numbers, and a presigned URL
            // is usable by anyone holding it, so this is capped far shorter than the staging database.
            var expiry = expiresInMinutes.HasValue
                ? TimeSpan.FromMinutes(Math.Clamp(expiresInMinutes.Value, 1, MaxSqlitePresignedUrlMinutes))
                : DefaultPresignedUrlExpiry;

            var presignedUrl = storageService.GeneratePresignedUrl(latest.Key, expiry);

            logger.LogInformation("Generated presigned URL for {Key} (expires in {ExpiryMinutes} min)",
                latest.Key, expiry.TotalMinutes);

            return Ok(new StagingDatabaseLatestResponse
            {
                ObjectKey = $"{EtlPipelineFolders.Views}/{latest.Key}",
                DownloadUrl = presignedUrl,
                Size = latest.Size,
                LastModified = latest.LastModified,
                ExpiresAt = DateTime.UtcNow.Add(expiry)
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get latest SQLite read model request was cancelled");
            return RequestCancelled();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error getting latest SQLite read model presigned URL");
            return PresignedUrlFailed();
        }
    }

    private OkObjectResult PresignedUrlReady(StorageObjectInfo latest, string presignedUrl, TimeSpan expiry)
        => Ok(new StagingDatabaseLatestResponse
        {
            ObjectKey = $"{EtlPipelineFolders.Staging}/{latest.Key}",
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
            logger.LogWarning("No DuckDB staging files found in {Folder}/", EtlPipelineFolders.Staging);
            return NotFound(new StagingDatabaseErrorResponse
            {
                Message = "No DuckDB staging databases found. Run the ETL pipeline first.",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    private NotFoundObjectResult SqliteNotFound
    {
        get
        {
            logger.LogWarning("No SQLite read model found in {Folder}/", EtlPipelineFolders.Views);
            return NotFound(new StagingDatabaseErrorResponse
            {
                Message = "No SQLite read model found. Run the ETL pipeline first.",
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
