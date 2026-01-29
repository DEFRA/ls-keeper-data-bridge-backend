using System.Text;
using FluentAssertions;
using KeeperData.Core.Crypto;

namespace KeeperData.Core.Tests.Unit.Crypto;

public class Md5HashHelperTests
{
    [Fact]
    public async Task CalculateMd5Async_WithValidStream_ReturnsCorrectHash()
    {
        // Arrange
        var content = "Hello, World!";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));

        // Act
        var hash = await Md5HashHelper.CalculateMd5Async(stream);

        // Assert
        hash.Should().Be("65a8e27d8879283831b664bd8b7f0ad4");
    }

    [Fact]
    public async Task CalculateMd5Async_ResetsStreamPositionAfterCalculation()
    {
        // Arrange
        var content = "Test content";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
        stream.Position = 5; // Set initial position

        // Act
        await Md5HashHelper.CalculateMd5Async(stream);

        // Assert
        stream.Position.Should().Be(5);
    }

    [Fact]
    public async Task CalculateMd5Async_WithEmptyStream_ReturnsEmptyStringHash()
    {
        // Arrange
        using var stream = new MemoryStream();

        // Act
        var hash = await Md5HashHelper.CalculateMd5Async(stream);

        // Assert - MD5 of empty string is d41d8cd98f00b204e9800998ecf8427e
        hash.Should().Be("d41d8cd98f00b204e9800998ecf8427e");
    }

    [Fact]
    public async Task CalculateMd5Async_WithNonSeekableStream_ThrowsArgumentException()
    {
        // Arrange
        using var nonSeekableStream = new NonSeekableStream();

        // Act
        var act = () => Md5HashHelper.CalculateMd5Async(nonSeekableStream);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>()
            .WithMessage("*seekable*");
    }

    [Fact]
    public async Task CalculateMd5WhileCopyingAsync_CopiesDataAndReturnsHash()
    {
        // Arrange
        var content = "Hello, World!";
        using var source = new MemoryStream(Encoding.UTF8.GetBytes(content));
        using var destination = new MemoryStream();

        // Act
        var hash = await Md5HashHelper.CalculateMd5WhileCopyingAsync(source, destination);

        // Assert
        hash.Should().Be("65a8e27d8879283831b664bd8b7f0ad4");
        destination.ToArray().Should().BeEquivalentTo(Encoding.UTF8.GetBytes(content));
    }

    [Fact]
    public async Task CalculateMd5WhileCopyingAsync_WithEmptySource_ReturnsEmptyHash()
    {
        // Arrange
        using var source = new MemoryStream();
        using var destination = new MemoryStream();

        // Act
        var hash = await Md5HashHelper.CalculateMd5WhileCopyingAsync(source, destination);

        // Assert
        hash.Should().Be("d41d8cd98f00b204e9800998ecf8427e");
        destination.Length.Should().Be(0);
    }

    [Fact]
    public async Task CalculateMd5Async_WithCancellationToken_CanBeCancelled()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        cts.Cancel();
        using var stream = new MemoryStream(new byte[1000]);

        // Act
        var act = () => Md5HashHelper.CalculateMd5Async(stream, cts.Token);

        // Assert
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    private class NonSeekableStream : MemoryStream
    {
        public override bool CanSeek => false;
    }
}
