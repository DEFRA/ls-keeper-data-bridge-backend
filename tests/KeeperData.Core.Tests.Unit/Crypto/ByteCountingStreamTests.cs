using FluentAssertions;
using KeeperData.Core.Crypto;

namespace KeeperData.Core.Tests.Unit.Crypto;

public class ByteCountingStreamTests
{
    [Fact]
    public void Constructor_WithNullStream_ThrowsArgumentNullException()
    {
        // Act
        var act = () => new ByteCountingStream(null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void BytesWritten_InitialValue_IsZero()
    {
        // Arrange
        using var inner = new MemoryStream();
        using var sut = new ByteCountingStream(inner);

        // Assert
        sut.BytesWritten.Should().Be(0);
    }

    [Fact]
    public void Write_TracksWrittenBytes()
    {
        // Arrange
        using var inner = new MemoryStream();
        using var sut = new ByteCountingStream(inner);
        var data = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        sut.Write(data, 0, data.Length);

        // Assert
        sut.BytesWritten.Should().Be(5);
    }

    [Fact]
    public async Task WriteAsync_ByteArray_TracksWrittenBytes()
    {
        // Arrange
        using var inner = new MemoryStream();
        using var sut = new ByteCountingStream(inner);
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        // Act
        await sut.WriteAsync(data, 0, data.Length);

        // Assert
        sut.BytesWritten.Should().Be(10);
    }

    [Fact]
    public async Task WriteAsync_Memory_TracksWrittenBytes()
    {
        // Arrange
        using var inner = new MemoryStream();
        using var sut = new ByteCountingStream(inner);
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
        ReadOnlyMemory<byte> memory = data;

        // Act
        await sut.WriteAsync(memory);

        // Assert
        sut.BytesWritten.Should().Be(7);
    }

    [Fact]
    public void MultipleWrites_AccumulateBytesWritten()
    {
        // Arrange
        using var inner = new MemoryStream();
        using var sut = new ByteCountingStream(inner);
        var data1 = new byte[] { 1, 2, 3 };
        var data2 = new byte[] { 4, 5, 6, 7, 8 };

        // Act
        sut.Write(data1, 0, data1.Length);
        sut.Write(data2, 0, data2.Length);

        // Assert
        sut.BytesWritten.Should().Be(8);
    }

    [Fact]
    public void CanRead_ReturnsInnerStreamValue()
    {
        // Arrange
        using var inner = new MemoryStream();
        using var sut = new ByteCountingStream(inner);

        // Assert
        sut.CanRead.Should().Be(inner.CanRead);
    }

    [Fact]
    public void CanSeek_ReturnsInnerStreamValue()
    {
        // Arrange
        using var inner = new MemoryStream();
        using var sut = new ByteCountingStream(inner);

        // Assert
        sut.CanSeek.Should().Be(inner.CanSeek);
    }

    [Fact]
    public void CanWrite_ReturnsInnerStreamValue()
    {
        // Arrange
        using var inner = new MemoryStream();
        using var sut = new ByteCountingStream(inner);

        // Assert
        sut.CanWrite.Should().Be(inner.CanWrite);
    }

    [Fact]
    public void Length_ReturnsInnerStreamLength()
    {
        // Arrange
        using var inner = new MemoryStream(new byte[100]);
        using var sut = new ByteCountingStream(inner);

        // Assert
        sut.Length.Should().Be(100);
    }

    [Fact]
    public void Position_GetAndSet_WorksCorrectly()
    {
        // Arrange
        using var inner = new MemoryStream(new byte[100]);
        using var sut = new ByteCountingStream(inner);

        // Act
        sut.Position = 50;

        // Assert
        sut.Position.Should().Be(50);
        inner.Position.Should().Be(50);
    }

    [Fact]
    public void Read_ReadsFromInnerStream()
    {
        // Arrange
        var data = new byte[] { 1, 2, 3, 4, 5 };
        using var inner = new MemoryStream(data);
        using var sut = new ByteCountingStream(inner);
        var buffer = new byte[5];

        // Act
        var bytesRead = sut.Read(buffer, 0, buffer.Length);

        // Assert
        bytesRead.Should().Be(5);
        buffer.Should().BeEquivalentTo(data);
    }

    [Fact]
    public async Task ReadAsync_ByteArray_ReadsFromInnerStream()
    {
        // Arrange
        var data = new byte[] { 1, 2, 3, 4, 5 };
        using var inner = new MemoryStream(data);
        using var sut = new ByteCountingStream(inner);
        var buffer = new byte[5];

        // Act
        var bytesRead = await sut.ReadAsync(buffer, 0, buffer.Length);

        // Assert
        bytesRead.Should().Be(5);
        buffer.Should().BeEquivalentTo(data);
    }

    [Fact]
    public async Task ReadAsync_Memory_ReadsFromInnerStream()
    {
        // Arrange
        var data = new byte[] { 1, 2, 3, 4, 5 };
        using var inner = new MemoryStream(data);
        using var sut = new ByteCountingStream(inner);
        var buffer = new byte[5];
        Memory<byte> memory = buffer;

        // Act
        var bytesRead = await sut.ReadAsync(memory);

        // Assert
        bytesRead.Should().Be(5);
        buffer.Should().BeEquivalentTo(data);
    }

    [Fact]
    public void Seek_SeeksInnerStream()
    {
        // Arrange
        using var inner = new MemoryStream(new byte[100]);
        using var sut = new ByteCountingStream(inner);

        // Act
        var newPosition = sut.Seek(25, SeekOrigin.Begin);

        // Assert
        newPosition.Should().Be(25);
        inner.Position.Should().Be(25);
    }

    [Fact]
    public void SetLength_SetsInnerStreamLength()
    {
        // Arrange
        using var inner = new MemoryStream();
        using var sut = new ByteCountingStream(inner);

        // Act
        sut.SetLength(50);

        // Assert
        inner.Length.Should().Be(50);
    }

    [Fact]
    public void Flush_FlushesInnerStream()
    {
        // Arrange
        using var inner = new MemoryStream();
        using var sut = new ByteCountingStream(inner);
        sut.Write(new byte[] { 1, 2, 3 }, 0, 3);

        // Act & Assert - should not throw
        var act = () => sut.Flush();
        act.Should().NotThrow();
    }

    [Fact]
    public async Task FlushAsync_FlushesInnerStream()
    {
        // Arrange
        using var inner = new MemoryStream();
        using var sut = new ByteCountingStream(inner);
        await sut.WriteAsync(new byte[] { 1, 2, 3 }, 0, 3);

        // Act & Assert - should not throw
        var act = () => sut.FlushAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void Dispose_DisposesInnerStream()
    {
        // Arrange
        var inner = new MemoryStream();
        var sut = new ByteCountingStream(inner);

        // Act
        sut.Dispose();

        // Assert - inner stream should be disposed
        var act = () => inner.Write(new byte[] { 1 }, 0, 1);
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public async Task DisposeAsync_DisposesInnerStream()
    {
        // Arrange
        var inner = new MemoryStream();
        var sut = new ByteCountingStream(inner);

        // Act
        await sut.DisposeAsync();

        // Assert - inner stream should be disposed
        var act = () => inner.Write(new byte[] { 1 }, 0, 1);
        act.Should().Throw<ObjectDisposedException>();
    }
}
