using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Parquet;

public class ParquetStreamsTests
{
    [Fact]
    public async Task AsSeekableAsync_returns_wrapped_non_disposing_stream_for_seekable_input()
    {
        var bytes = Enumerable.Range(0, 64).Select(i => (byte)i).ToArray();
        await using var ms = new MemoryStream(bytes, writable: false);
        // move position away from start to ensure method resets to 0
        ms.Position = 10;

var result = await KeeperData.Core.EtlPipeline.Parquet.ParquetStreams.AsSeekableAsync(ms, CancellationToken.None);

// position should have been reset on the original and exposed instance
        ms.Position.Should().Be(0);
        result.Position.Should().Be(0);

        result.CanSeek.Should().BeTrue();
        result.CanRead.Should().BeTrue();

result.CanWrite.Should().BeFalse(); // wrapper forbids writing

// the returned type should be the private NonDisposingStream nested type
        var nested = typeof(KeeperData.Core.EtlPipeline.Parquet.ParquetStreams).GetNestedType("NonDisposingStream", BindingFlags.NonPublic);
        nested.Should().NotBeNull();
        result.GetType().Should().Be(nested);

        // ensure reading returns the same bytes
        var buffer = new byte[bytes.Length];
        var read = await result.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None);
        read.Should().Be(bytes.Length);
        buffer.Should().Equal(bytes);

        // disposing the wrapper should not throw; the original stream is disposed by the test using block
        await using (result) { }
    }

    [Fact]
    public async Task AsSeekableAsync_buffers_non_seekable_stream_into_memory()
    {
        var bytes = Enumerable.Range(0, 128).Select(i => (byte)i).ToArray();
        // non-seekable wrapper around a MemoryStream to simulate an object download stream
        await using var inner = new MemoryStream(bytes, writable: false);
        await using var nonSeekable = new DelegatingNonSeekableStream(inner);

        var result = await KeeperData.Core.EtlPipeline.Parquet.ParquetStreams.AsSeekableAsync(nonSeekable, CancellationToken.None);

        // result should be a MemoryStream positioned at start with the same content
        result.Should().BeOfType<MemoryStream>();
        result.Position.Should().Be(0);

        var buffer = new byte[bytes.Length];
        var read = await result.ReadAsync(buffer, 0, buffer.Length, CancellationToken.None);
        read.Should().Be(bytes.Length);
        buffer.Should().Equal(bytes);

        await using (result) { }
    }

    private sealed class DelegatingNonSeekableStream : Stream
    {
        private readonly Stream _inner;
        public DelegatingNonSeekableStream(Stream inner) => _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => _inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => _inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

        protected override void Dispose(bool disposing) { /* do not dispose inner */ }
    }
}
