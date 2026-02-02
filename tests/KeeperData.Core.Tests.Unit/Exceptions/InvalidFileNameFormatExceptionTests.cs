using FluentAssertions;
using KeeperData.Core.Exceptions;

namespace KeeperData.Core.Tests.Unit.Exceptions;

public class InvalidFileNameFormatExceptionTests
{
    [Fact]
    public void Constructor_WithFileName_SetsDefaultMessage()
    {
        // Arrange
        var fileName = "invalid_file.csv";

        // Act
        var exception = new InvalidFileNameFormatException(fileName);

        // Assert
        exception.Message.Should().Contain(fileName);
        exception.Message.Should().Contain("does not contain the required format");
        exception.Message.Should().Contain("yyyy-MM-dd");
    }

    [Fact]
    public void Constructor_WithFileNameAndCustomMessage_SetsCustomMessage()
    {
        // Arrange
        var fileName = "invalid_file.csv";
        var customMessage = "Custom error message for the file";

        // Act
        var exception = new InvalidFileNameFormatException(fileName, customMessage);

        // Assert
        exception.Message.Should().Be(customMessage);
    }

    [Fact]
    public void Exception_IsOfTypeException()
    {
        // Arrange & Act
        var exception = new InvalidFileNameFormatException("test.csv");

        // Assert
        exception.Should().BeAssignableTo<Exception>();
    }
}
