using System.Security.Cryptography;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.Storage;

/// <summary>
/// A write stream backed by a local file that writes sidecar metadata on dispose.
/// Satisfies the <see cref="IBlobStorageService.OpenWriteAsync"/> contract where
/// disposing the stream finalizes the "upload".
/// </summary>
internal sealed class FileSystemWriteStream : Stream
{
    private readonly FileStream _fileStream;
    private readonly string _filePath;
    private readonly string? _contentType;
    private readonly IReadOnlyDictionary<string, string>? _metadata;
    private readonly ILogger _logger;
    private bool _disposed;

    public FileSystemWriteStream(
        string filePath,
        string? contentType,
        IReadOnlyDictionary<string, string>? metadata,
        ILogger logger)
    {
        _filePath = filePath;
        _contentType = contentType;
        _metadata = metadata;
        _logger = logger;
        _fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 81920, useAsync: true);
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _fileStream.Length;
    public override long Position
    {
        get => _fileStream.Position;
        set => throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
        => _fileStream.Write(buffer, offset, count);

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => _fileStream.WriteAsync(buffer, offset, count, cancellationToken);

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        => _fileStream.WriteAsync(buffer, cancellationToken);

    public override void Flush() => _fileStream.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => _fileStream.FlushAsync(cancellationToken);

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            _disposed = true;
            _fileStream.Dispose();
            WriteSidecarMetadata();
        }

        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await _fileStream.DisposeAsync().ConfigureAwait(false);
            await WriteSidecarMetadataAsync().ConfigureAwait(false);
        }

        await base.DisposeAsync().ConfigureAwait(false);
    }

    private void WriteSidecarMetadata()
    {
        var etag = ComputeETag();
        FileSystemBlobStorageService.WriteSidecarMetadata(_filePath, _contentType, _metadata, etag);
        _logger.LogDebug("Wrote sidecar metadata to {MetaPath}", _filePath + ".meta.json");
    }

    private async Task WriteSidecarMetadataAsync()
    {
        var etag = ComputeETag();
        FileSystemBlobStorageService.WriteSidecarMetadata(_filePath, _contentType, _metadata, etag);
        _logger.LogDebug("Wrote sidecar metadata to {MetaPath}", _filePath + ".meta.json");
        await Task.CompletedTask.ConfigureAwait(false);
    }

    private string ComputeETag()
    {
        using var stream = File.OpenRead(_filePath);
        var hash = MD5.HashData(stream);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }
}
