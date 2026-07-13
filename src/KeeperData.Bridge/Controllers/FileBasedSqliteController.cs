using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Storage;
using Microsoft.AspNetCore.Mvc;

namespace KeeperData.Bridge.Controllers;

[ApiController]
[Route("api/etl/file-based/sqlite")]
[ExcludeFromCodeCoverage(Justification = "API controller - covered by component/integration tests.")]
public class FileBasedSqliteController(
    IBlobStorageServiceFactory blobStorageServiceFactory,
    ILogger<FileBasedSqliteController> logger) : ControllerBase
{
    private const string ViewsPrefix = "views/";
    private const string CphsSqlitePattern = "cphs_";
    private const string SqliteExtension = ".sqlite";
    private static readonly TimeSpan DefaultPresignedUrlExpiry = TimeSpan.FromHours(1);

    /// <summary>
    /// Returns a presigned download URL for the latest CPH SQLite file.
    /// The URL is valid for 1 hour by default.
    /// </summary>
    /// <param name="expiresInMinutes">Optional expiry in minutes (default: 60, max: 10080 / 7 days)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    [HttpGet("cphs/latest")]
    [ProducesResponseType(typeof(CphSqliteLatestResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(CphSqliteErrorResponse), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(CphSqliteErrorResponse), StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> GetLatestCphSqliteUrl(
        [FromQuery] int? expiresInMinutes = null,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Received request for latest CPH SQLite presigned URL");

        try
        {
            var storageService = blobStorageServiceFactory.GetSourceInternal();
            var objects = await storageService.ListAsync(ViewsPrefix, cancellationToken);

            var latestSqlite = objects
                .Where(o => o.Key.Contains(CphsSqlitePattern) &&
                            o.Key.EndsWith(SqliteExtension, StringComparison.OrdinalIgnoreCase))
                .OrderByDescending(o => o.Key)
                .FirstOrDefault();

            if (latestSqlite is null)
            {
                logger.LogWarning("No CPH SQLite files found in {Prefix}", ViewsPrefix);
                return NotFound(new CphSqliteErrorResponse
                {
                    Message = "No CPH SQLite export files found. Trigger an export first via POST /api/etl/file-based/exports/cphs.",
                    Timestamp = DateTime.UtcNow
                });
            }

            var expiry = expiresInMinutes.HasValue
                ? TimeSpan.FromMinutes(Math.Clamp(expiresInMinutes.Value, 1, 10080))
                : DefaultPresignedUrlExpiry;

            var presignedUrl = storageService.GeneratePresignedUrl(latestSqlite.Key, expiry);

            logger.LogInformation("Generated presigned URL for {Key} (expires in {ExpiryMinutes} min)",
                latestSqlite.Key, expiry.TotalMinutes);

            return Ok(new CphSqliteLatestResponse
            {
                ObjectKey = latestSqlite.Key,
                DownloadUrl = presignedUrl,
                Size = latestSqlite.Size,
                LastModified = latestSqlite.LastModified,
                ExpiresAt = DateTime.UtcNow.Add(expiry)
            });
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Get latest CPH SQLite request was cancelled");
            return StatusCode(499, new CphSqliteErrorResponse
            {
                Message = "Request was cancelled.",
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error getting latest CPH SQLite presigned URL");
            return StatusCode(StatusCodes.Status500InternalServerError, new CphSqliteErrorResponse
            {
                Message = "An unexpected error occurred while generating the download URL.",
                Timestamp = DateTime.UtcNow
            });
        }
    }
}

#region Response DTOs

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record CphSqliteLatestResponse
{
    public required string ObjectKey { get; init; }
    public required string DownloadUrl { get; init; }
    public long Size { get; init; }
    public DateTimeOffset LastModified { get; init; }
    public DateTime ExpiresAt { get; init; }
}

[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record CphSqliteErrorResponse
{
    public required string Message { get; init; }
    public DateTime Timestamp { get; init; }
}

#endregion
