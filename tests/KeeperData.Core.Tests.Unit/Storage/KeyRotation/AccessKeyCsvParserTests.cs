using FluentAssertions;
using KeeperData.Core.Storage.KeyRotation;
using System.Text;

namespace KeeperData.Core.Tests.Unit.Storage.KeyRotation;

public class AccessKeyCsvParserTests
{
    private const string ValidKeyId = "AKIA1234567890ABCDEF";
    private const string ValidSecret = "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY";

    private static Stream ToStream(string content) => new MemoryStream(Encoding.UTF8.GetBytes(content));

    [Fact]
    public void Parse_WithWellFormedFile_ReturnsKeyAndSecret()
    {
        // Arrange
        var csv = $"Access key ID,Secret access key\n{ValidKeyId},{ValidSecret}\n";

        // Act
        var result = AccessKeyCsvParser.Parse(ToStream(csv));

        // Assert
        result.AccessKeyId.Should().Be(ValidKeyId);
        result.SecretAccessKey.Should().Be(ValidSecret);
    }

    [Fact]
    public void Parse_WithWindowsLineEndingsAndBom_ReturnsKeyAndSecret()
    {
        // Arrange
        var csv = $"\uFEFFAccess key ID,Secret access key\r\n{ValidKeyId},{ValidSecret}\r\n";

        // Act
        var result = AccessKeyCsvParser.Parse(ToStream(csv));

        // Assert
        result.AccessKeyId.Should().Be(ValidKeyId);
        result.SecretAccessKey.Should().Be(ValidSecret);
    }

    [Fact]
    public void Parse_WithExtraMiddleColumns_TakesFirstAndLast()
    {
        // Arrange
        var csv = $"Access key ID,User,Secret access key\n{ValidKeyId},someone,{ValidSecret}\n";

        // Act
        var result = AccessKeyCsvParser.Parse(ToStream(csv));

        // Assert
        result.AccessKeyId.Should().Be(ValidKeyId);
        result.SecretAccessKey.Should().Be(ValidSecret);
    }

    [Fact]
    public void Parse_WithWhitespaceAroundValues_TrimsValues()
    {
        // Arrange
        var csv = $"Access key ID,Secret access key\n {ValidKeyId} , {ValidSecret} \n";

        // Act
        var result = AccessKeyCsvParser.Parse(ToStream(csv));

        // Assert
        result.AccessKeyId.Should().Be(ValidKeyId);
        result.SecretAccessKey.Should().Be(ValidSecret);
    }

    [Fact]
    public void Parse_WithEmptyFile_Throws()
    {
        // Act
        var act = () => AccessKeyCsvParser.Parse(ToStream(""));

        // Assert
        act.Should().Throw<AccessKeyFileFormatException>().WithMessage("*empty*");
    }

    [Fact]
    public void Parse_WithHeaderOnly_Throws()
    {
        // Act
        var act = () => AccessKeyCsvParser.Parse(ToStream("Access key ID,Secret access key\n"));

        // Assert
        act.Should().Throw<AccessKeyFileFormatException>().WithMessage("*no data row*");
    }

    [Fact]
    public void Parse_WithMultipleDataRows_Throws()
    {
        // Arrange
        var csv = $"Access key ID,Secret access key\n{ValidKeyId},{ValidSecret}\nAKIA0987654321FEDCBA,other\n";

        // Act
        var act = () => AccessKeyCsvParser.Parse(ToStream(csv));

        // Assert
        act.Should().Throw<AccessKeyFileFormatException>().WithMessage("*more than one data row*");
    }

    [Fact]
    public void Parse_WithSingleColumn_Throws()
    {
        // Act
        var act = () => AccessKeyCsvParser.Parse(ToStream($"Access key ID\n{ValidKeyId}\n"));

        // Assert
        act.Should().Throw<AccessKeyFileFormatException>().WithMessage("*at least 2*");
    }

    [Theory]
    [InlineData("short")]
    [InlineData("lowercasekeyid123456")]
    [InlineData("HAS SPACES IN THE KEY")]
    public void Parse_WithInvalidKeyIdFormat_Throws(string badKeyId)
    {
        // Arrange
        var csv = $"Access key ID,Secret access key\n{badKeyId},{ValidSecret}\n";

        // Act
        var act = () => AccessKeyCsvParser.Parse(ToStream(csv));

        // Assert
        act.Should().Throw<AccessKeyFileFormatException>().WithMessage("*format validation*");
    }

    [Fact]
    public void Parse_WithEmptySecret_Throws()
    {
        // Arrange
        var csv = $"Access key ID,Secret access key\n{ValidKeyId},\n";

        // Act
        var act = () => AccessKeyCsvParser.Parse(ToStream(csv));

        // Assert
        act.Should().Throw<AccessKeyFileFormatException>().WithMessage("*secret access key column is empty*");
    }

    [Fact]
    public void Parse_ExceptionMessages_NeverContainFileValues()
    {
        // Arrange
        var csv = $"Access key ID,Secret access key\nbadkey,{ValidSecret}\n";

        // Act
        var act = () => AccessKeyCsvParser.Parse(ToStream(csv));

        // Assert
        act.Should().Throw<AccessKeyFileFormatException>()
            .Which.Message.Should().NotContainAny("badkey", ValidSecret);
    }

    [Fact]
    public void Parse_WithTrailingBlankLines_Succeeds()
    {
        // Arrange
        var csv = $"Access key ID,Secret access key\n{ValidKeyId},{ValidSecret}\n\n";

        // Act
        var result = AccessKeyCsvParser.Parse(ToStream(csv));

        // Assert
        result.AccessKeyId.Should().Be(ValidKeyId);
    }
}
