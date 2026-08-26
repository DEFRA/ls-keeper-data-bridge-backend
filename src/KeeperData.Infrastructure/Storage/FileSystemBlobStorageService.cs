using System.Security.Cryptography;
using System.Text.Json;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.Storage;

public class FileSystemBlobStorageService : IBlobStorageService
{
    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    private readonly ILogger _logger;
    private readonly string _basePath;
    private readonly string? _topLevelFolder;

    public FileSystemBlobStorageService(
        ILogger logger,
        string basePath,
        string? topLevelFolder = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _basePath = basePath ?? throw new ArgumentNullException(nameof(basePath));
        _topLevelFolder = NormalizeTopLevelFolder(topLevelFolder);
    }

    public Task<IReadOnlyList<StorageObjectInfo>> ListAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default)
    {
        var rootDir = GetRootDirectory();

        if (!Directory.Exists(rootDir))
            return Task.FromResult<IReadOnlyList<StorageObjectInfo>>(Array.Empty<StorageObjectInfo>());

        var files = Directory.EnumerateFiles(rootDir, "*", SearchOption.AllDirectories)
            .Where(f => !IsMetadataFile(f))
            .Select(f => CreateStorageObjectInfo(f))
            .Where(info => string.IsNullOrEmpty(prefix) || info.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            .ToList();

        return Task.FromResult<IReadOnlyList<StorageObjectInfo>>(files);
    }

    public Task<StorageListPage> ListPageAsync(
        string? prefix = null,
        int pageSize = 1000,
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
    {
        var rootDir = GetRootDirectory();

        if (!Directory.Exists(rootDir))
        {
            return Task.FromResult(new StorageListPage
            {
                Items = Array.Empty<StorageObjectInfo>(),
                ContinuationToken = null,
                IsTruncated = false
            });
        }

        var allFiles = Directory.EnumerateFiles(rootDir, "*", SearchOption.AllDirectories)
            .Where(f => !IsMetadataFile(f))
            .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
            .Select(f => CreateStorageObjectInfo(f))
            .Where(info => string.IsNullOrEmpty(prefix) || info.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            .ToList();

        var skip = 0;
        if (!string.IsNullOrEmpty(continuationToken) && int.TryParse(continuationToken, out var parsedSkip))
            skip = parsedSkip;

        var clampedPageSize = Math.Min(Math.Max(pageSize, 1), 1000);
        var page = allFiles.Skip(skip).Take(clampedPageSize).ToList();
        var nextSkip = skip + page.Count;
        var hasMore = nextSkip < allFiles.Count;

        return Task.FromResult(new StorageListPage
        {
            Items = page,
            ContinuationToken = hasMore ? nextSkip.ToString() : null,
            IsTruncated = hasMore
        });
    }

    public Task<StorageObjectMetadata> GetMetadataAsync(
        string objectKey,
        CancellationToken cancellationToken = default)
    {
        var filePath = GetFullFilePath(objectKey);

        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Object not found: {objectKey}", filePath);

        var fileInfo = new FileInfo(filePath);
        var sidecar = ReadSidecarMetadata(filePath);
        var etag = sidecar.ETag ?? ComputeFileETag(filePath);

        var metadata = new StorageObjectMetadata
        {
            Container = _basePath,
            Key = objectKey,
            ContentLength = fileInfo.Length,
            ContentType = sidecar.ContentType ?? "application/octet-stream",
            ETag = etag,
            LastModified = fileInfo.LastWriteTimeUtc,
            StorageClass = "FILESYSTEM",
            Encryption = null,
            StorageUri = new Uri($"file:///{filePath.Replace('\\', '/')}"),
            HttpUri = null,
            UserMetadata = sidecar.UserMetadata ?? new Dictionary<string, string>(),
            ProviderProperties = new Dictionary<string, string>
            {
                ["StorageClass"] = "FILESYSTEM"
            }
        };

        return Task.FromResult(metadata);
    }

    public async Task<byte[]> DownloadAsync(
        string objectKey,
        CancellationToken cancellationToken = default)
    {
        var filePath = GetFullFilePath(objectKey);

        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Object not found: {objectKey}", filePath);

        return await File.ReadAllBytesAsync(filePath, cancellationToken).ConfigureAwait(false);
    }

    public Task<Stream> OpenReadAsync(
        string objectKey,
        CancellationToken cancellationToken = default)
    {
        var filePath = GetFullFilePath(objectKey);

        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Object not found: {objectKey}", filePath);

        var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, useAsync: true);
        return Task.FromResult<Stream>(stream);
    }

    public Task<bool> ExistsAsync(
        string objectKey,
        CancellationToken cancellationToken = default)
    {
        var filePath = GetFullFilePath(objectKey);
        return Task.FromResult(File.Exists(filePath));
    }

    public string GeneratePresignedUrl(string objectKey, TimeSpan? expiresIn = null)
    {
        var filePath = GetFullFilePath(objectKey);
        return new Uri(filePath).AbsoluteUri;
    }

    public async Task UploadAsync(
        string objectKey,
        byte[] content,
        string? contentType = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        CancellationToken cancellationToken = default)
    {
        var filePath = GetFullFilePath(objectKey);
        EnsureDirectoryExists(filePath);

        await File.WriteAllBytesAsync(filePath, content, cancellationToken).ConfigureAwait(false);

        var etag = ComputeFileETag(filePath);
        WriteSidecarMetadata(filePath, contentType, metadata, etag);

        _logger.LogDebug("Successfully uploaded object {ObjectKey} to {BasePath}", objectKey, _basePath);
    }

    public Task<Stream> OpenWriteAsync(
        string objectKey,
        string? contentType = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        int partSizeBytes = 8 * 1024 * 1024,
        CancellationToken cancellationToken = default)
    {
        var filePath = GetFullFilePath(objectKey);
        EnsureDirectoryExists(filePath);

        var stream = new FileSystemWriteStream(filePath, contentType, metadata, _logger);
        return Task.FromResult<Stream>(stream);
    }

    public Task SetMetadataAsync(
        string objectKey,
        IReadOnlyDictionary<string, string> metadata,
        CancellationToken cancellationToken = default)
    {
        var filePath = GetFullFilePath(objectKey);

        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Object not found: {objectKey}", filePath);

        var existing = ReadSidecarMetadata(filePath);
        WriteSidecarMetadata(filePath, existing.ContentType, metadata, existing.ETag);

        _logger.LogDebug("Successfully updated metadata for object {ObjectKey} in {BasePath}", objectKey, _basePath);
        return Task.CompletedTask;
    }

    public Task DeleteAsync(string objectKey, CancellationToken cancellationToken = default)
    {
        var filePath = GetFullFilePath(objectKey);
        var metaPath = GetMetadataFilePath(filePath);

        if (File.Exists(filePath))
            File.Delete(filePath);

        if (File.Exists(metaPath))
            File.Delete(metaPath);

        _logger.LogDebug("Successfully deleted object {ObjectKey} from {BasePath}", objectKey, _basePath);
        return Task.CompletedTask;
    }

    public async Task<ClearDownResult> DeleteByPrefixAsync(
        string prefix,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);

        var objects = await ListAsync(prefix, cancellationToken).ConfigureAwait(false);
        var deletedKeys = new List<string>(objects.Count);

        foreach (var item in objects)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await DeleteAsync(item.Key, cancellationToken).ConfigureAwait(false);
            deletedKeys.Add(item.Key);
        }

        var rootDir = GetRootDirectory();
        if (Directory.Exists(rootDir))
            CleanEmptyDirectories(rootDir);

        _logger.LogInformation(
            "Prefix deletion completed for {BasePath}, prefix {Prefix}. Total objects deleted: {TotalDeleted}",
            _basePath,
            prefix,
            deletedKeys.Count);

        return new ClearDownResult
        {
            DeletedKeys = deletedKeys,
            TotalDeleted = deletedKeys.Count
        };
    }

    public Task<ClearDownResult> ClearDownAsync(CancellationToken cancellationToken = default)
    {
        var rootDir = GetRootDirectory();
        var deletedKeys = new List<string>();

        if (Directory.Exists(rootDir))
        {
            var files = Directory.EnumerateFiles(rootDir, "*", SearchOption.AllDirectories)
                .Where(f => !IsMetadataFile(f))
                .ToList();

            foreach (var file in files)
            {
                var key = GetRelativeKey(file);
                deletedKeys.Add(key);

                File.Delete(file);
                var metaPath = GetMetadataFilePath(file);
                if (File.Exists(metaPath))
                    File.Delete(metaPath);
            }

            CleanEmptyDirectories(rootDir);
        }

        _logger.LogInformation("Clear down completed for {BasePath}. Total objects deleted: {TotalDeleted}",
            _basePath, deletedKeys.Count);

        return Task.FromResult(new ClearDownResult
        {
            DeletedKeys = deletedKeys,
            TotalDeleted = deletedKeys.Count
        });
    }

    // --- Path helpers ---

    private string GetRootDirectory()
    {
        return string.IsNullOrEmpty(_topLevelFolder)
            ? _basePath
            : Path.Combine(_basePath, _topLevelFolder);
    }

    private string GetFullFilePath(string objectKey)
    {
        return Path.Combine(GetRootDirectory(), NormalizeKeyToPath(objectKey));
    }

    private string GetRelativeKey(string fullPath)
    {
        var rootDir = GetRootDirectory();
        var relative = Path.GetRelativePath(rootDir, fullPath);
        return relative.Replace('\\', '/');
    }

    private static string NormalizeKeyToPath(string key)
    {
        return key.Replace('/', Path.DirectorySeparatorChar)
                  .Replace('\\', Path.DirectorySeparatorChar)
                  .TrimStart(Path.DirectorySeparatorChar);
    }

    private static void EnsureDirectoryExists(string filePath)
    {
        var dir = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            Directory.CreateDirectory(dir);
    }

    private static string? NormalizeTopLevelFolder(string? topLevelFolder)
    {
        if (string.IsNullOrWhiteSpace(topLevelFolder))
            return null;

        var normalized = topLevelFolder.Trim().Trim('/');
        return string.IsNullOrEmpty(normalized) ? null : normalized;
    }

    // --- Metadata sidecar helpers ---

    private static string GetMetadataFilePath(string dataFilePath)
    {
        return dataFilePath + ".meta.json";
    }

    private static bool IsMetadataFile(string filePath)
    {
        return filePath.EndsWith(".meta.json", StringComparison.OrdinalIgnoreCase);
    }

    private static SidecarMetadata ReadSidecarMetadata(string dataFilePath)
    {
        var metaPath = GetMetadataFilePath(dataFilePath);
        if (!File.Exists(metaPath))
            return new SidecarMetadata();

        var json = File.ReadAllText(metaPath);
        return JsonSerializer.Deserialize<SidecarMetadata>(json) ?? new SidecarMetadata();
    }

    internal static void WriteSidecarMetadata(string dataFilePath, string? contentType, IReadOnlyDictionary<string, string>? metadata, string? etag = null)
    {
        var metaPath = GetMetadataFilePath(dataFilePath);
        var sidecar = new SidecarMetadata
        {
            ContentType = contentType,
            ETag = etag,
            UserMetadata = metadata != null ? new Dictionary<string, string>(metadata) : new Dictionary<string, string>()
        };

        var json = JsonSerializer.Serialize(sidecar, s_jsonOptions);
        File.WriteAllText(metaPath, json);
    }

    private static string ComputeFileETag(string filePath)
    {
        using var stream = File.OpenRead(filePath);
        var hash = SHA256.HashData(stream);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }

    private StorageObjectInfo CreateStorageObjectInfo(string fullPath)
    {
        var fileInfo = new FileInfo(fullPath);
        var key = GetRelativeKey(fullPath);
        var sidecar = ReadSidecarMetadata(fullPath);

        return new StorageObjectInfo
        {
            Container = _basePath,
            Key = key,
            Size = fileInfo.Length,
            LastModified = fileInfo.LastWriteTimeUtc,
            ETag = sidecar.ETag ?? ComputeFileETag(fullPath),
            StorageUri = new Uri($"file:///{fullPath.Replace('\\', '/')}"),
            HttpUri = null
        };
    }

    private static void CleanEmptyDirectories(string rootDir)
    {
        foreach (var dir in Directory.EnumerateDirectories(rootDir, "*", SearchOption.AllDirectories)
            .OrderByDescending(d => d.Length)
            .Where(dir => !Directory.EnumerateFileSystemEntries(dir).Any()))
        {
            Directory.Delete(dir);
        }
    }

    public override string ToString() => $"BlobStorageService(fs={_basePath},tlf={_topLevelFolder})";
}
