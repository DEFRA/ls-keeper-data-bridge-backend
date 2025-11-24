using FluentAssertions;
using KeeperData.Core.Querying.Impl;

namespace KeeperData.Core.Tests.Unit.Querying;

public class ODataSelectParserTests
{
    private readonly ODataSelectParser _sut;

    public ODataSelectParserTests()
    {
        _sut = new ODataSelectParser();
    }

    [Fact]
    public void Parse_WithNull_ReturnsNull()
    {
        // Act
        var result = _sut.Parse(null);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void Parse_WithEmptyString_ReturnsNull()
    {
        // Act
        var result = _sut.Parse("");

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void Parse_WithWhitespace_ReturnsNull()
    {
        // Act
        var result = _sut.Parse("   ");

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void Parse_WithSingleField_ReturnsSingleField()
    {
        // Act
        var result = _sut.Parse("FieldName");

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(1);
        result![0].Should().Be("FieldName");
    }

    [Fact]
    public void Parse_WithMultipleFields_ReturnsAllFields()
    {
        // Act
        var result = _sut.Parse("Field1,Field2,Field3");

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(3);
        result![0].Should().Be("Field1");
        result[1].Should().Be("Field2");
        result[2].Should().Be("Field3");
    }

    [Fact]
    public void Parse_WithSpaces_TrimsFields()
    {
        // Act
        var result = _sut.Parse("Field1 , Field2 , Field3");

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(3);
        result![0].Should().Be("Field1");
        result[1].Should().Be("Field2");
        result[2].Should().Be("Field3");
    }

    [Fact]
    public void Parse_WithNestedFields_AcceptsDotsInFieldNames()
    {
        // Act
        var result = _sut.Parse("Parent.Child,Other.Nested.Field");

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(2);
        result![0].Should().Be("Parent.Child");
        result[1].Should().Be("Other.Nested.Field");
    }

    [Fact]
    public void Parse_WithUnderscores_AcceptsUnderscoresInFieldNames()
    {
        // Act
        var result = _sut.Parse("_id,field_name,another_field");

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(3);
        result![0].Should().Be("_id");
        result[1].Should().Be("field_name");
        result[2].Should().Be("another_field");
    }

    [Fact]
    public void Parse_WithOnlyCommas_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => _sut.Parse(",,,"));
        exception.Message.Should().Contain("must contain at least one field");
    }

    [Fact]
    public void Parse_WithInvalidFieldName_StartingWithDigit_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => _sut.Parse("123Field"));
        exception.Message.Should().Contain("Invalid field name");
    }

    [Fact]
    public void Parse_WithInvalidFieldName_ContainingSpecialChars_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => _sut.Parse("Field@Name"));
        exception.Message.Should().Contain("Invalid field name");
    }

    [Fact]
    public void Parse_WithMixedValidAndInvalidFields_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => _sut.Parse("ValidField,Invalid@Field"));
        exception.Message.Should().Contain("Invalid field name");
    }

    [Theory]
    [InlineData("CPH")]
    [InlineData("CPH,UpdatedAtUtc")]
    [InlineData("CPH,UpdatedAtUtc,IsDeleted")]
    [InlineData("_id,ProductId,Name,Category,Price")]
    public void Parse_WithValidFieldLists_ReturnsCorrectCount(string input)
    {
        // Act
        var result = _sut.Parse(input);

        // Assert
        var expectedCount = input.Split(',').Length;
        result.Should().NotBeNull();
        result.Should().HaveCount(expectedCount);
    }
}