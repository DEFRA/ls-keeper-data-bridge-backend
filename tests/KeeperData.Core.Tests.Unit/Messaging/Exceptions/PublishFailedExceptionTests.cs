using FluentAssertions;
using KeeperData.Core.Messaging.Exceptions;

namespace KeeperData.Core.Tests.Unit.Messaging.Exceptions;

public class PublishFailedExceptionTests
{
    [Fact]
    public void Constructor_WithMessageAndIsTransient_SetsProperties()
    {
        // Arrange
        var message = "Failed to publish message";
        var isTransient = true;

        // Act
        var exception = new PublishFailedException(message, isTransient);

        // Assert
        exception.Message.Should().Be(message);
        exception.IsTransient.Should().BeTrue();
        exception.InnerException.Should().BeNull();
    }

    [Fact]
    public void Constructor_WithMessageIsTransientAndInnerException_SetsAllProperties()
    {
        // Arrange
        var message = "Failed to publish message";
        var isTransient = false;
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = new PublishFailedException(message, isTransient, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.IsTransient.Should().BeFalse();
        exception.InnerException.Should().Be(innerException);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IsTransient_ReturnsCorrectValue(bool isTransient)
    {
        // Arrange & Act
        var exception = new PublishFailedException("Error", isTransient);

        // Assert
        exception.IsTransient.Should().Be(isTransient);
    }

    [Fact]
    public void Exception_IsOfTypeException()
    {
        // Arrange & Act
        var exception = new PublishFailedException("Error", true);

        // Assert
        exception.Should().BeAssignableTo<Exception>();
    }
}
