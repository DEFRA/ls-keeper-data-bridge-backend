namespace KeeperData.Core.EtlPipeline.Storage;

/// <summary>Replays an already-read head buffer ahead of the rest of a forward-only source, so a
/// stage can inspect the start of a file and still hand the whole of it to a parser. The source is
/// left open: its owner disposes it.</summary>
public sealed class HeadPeekStream(ReadOnlyMemory<byte> head, Stream source) : Stream
{
    private int _headOffset;

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
        => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        if (buffer.IsEmpty)
        {
            return 0;
        }

        var remaining = head.Length - _headOffset;
        if (remaining > 0)
        {
            var take = Math.Min(remaining, buffer.Length);
            head.Span.Slice(_headOffset, take).CopyTo(buffer);
            _headOffset += take;
            return take;
        }

        return source.Read(buffer);
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.IsEmpty)
        {
            return 0;
        }

        var remaining = head.Length - _headOffset;
        if (remaining > 0)
        {
            var take = Math.Min(remaining, buffer.Length);
            head.Slice(_headOffset, take).CopyTo(buffer);
            _headOffset += take;
            return take;
        }

        return await source.ReadAsync(buffer, cancellationToken);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override void Flush() { }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
