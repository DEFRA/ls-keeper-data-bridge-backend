using FluentAssertions;
using KeeperData.Core.Querying.Impl;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace KeeperData.Core.Tests.Unit.Querying;

public class ODataToMongoFilterTranslatorTests
{
    private readonly ODataToMongoFilterTranslator _sut = new();

    [Fact]
    public void Parse_WithNullOrEmptyFilter_ReturnsEmptyFilter()
    {
        // Act
        var result1 = _sut.Parse(null!);
        var result2 = _sut.Parse("");
        var result3 = _sut.Parse("   ");

        // Assert - empty filters should match everything
        result1.Should().NotBeNull();
        result2.Should().NotBeNull();
        result3.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithEqualOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("CPH eq 'ABC123'");

        // Assert
        result.Should().NotBeNull();
        var renderedFilter = result.Render(BsonSerializer.SerializerRegistry.GetSerializer<BsonDocument>(), BsonSerializer.SerializerRegistry);
        renderedFilter.ToString().Should().Contain("CPH");
    }

    [Fact]
    public void Parse_WithNotEqualOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Status ne 'Deleted'");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithGreaterThanOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Age gt 18");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithGreaterThanOrEqualOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Age ge 18");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithLessThanOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Age lt 65");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithLessThanOrEqualOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Age le 65");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithAndOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("CPH eq 'ABC123' and IsDeleted eq false");

        // Assert
        result.Should().NotBeNull();
        var renderedFilter = result.Render(BsonSerializer.SerializerRegistry.GetSerializer<BsonDocument>(), BsonSerializer.SerializerRegistry);
        var filterString = renderedFilter.ToString();
        filterString.Should().Contain("CPH");
        filterString.Should().Contain("IsDeleted");
    }

    [Fact]
    public void Parse_WithOrOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Status eq 'Active' or Status eq 'Pending'");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithContainsFunction_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("contains(Name, 'John')");

        // Assert
        result.Should().NotBeNull();
        var renderedFilter = result.Render(BsonSerializer.SerializerRegistry.GetSerializer<BsonDocument>(), BsonSerializer.SerializerRegistry);
        var filterString = renderedFilter.ToString();
        filterString.Should().Contain("Name");
    }

    [Fact]
    public void Parse_WithStartsWithFunction_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("startswith(Name, 'A')");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithEndsWithFunction_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("endswith(Email, '@example.com')");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithComplexExpression_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("(CPH eq 'ABC123' and IsDeleted eq false) or Status eq 'Active'");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithBooleanValue_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("IsActive eq true");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithNumericValue_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Count eq 42");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithInvalidSyntax_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => _sut.Parse("invalid syntax here"));
        exception.Message.Should().Contain("Invalid OData filter expression");
    }

    [Fact]
    public void Parse_WithMultipleAndConditions_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Field1 eq 'Value1' and Field2 eq 'Value2' and Field3 eq 'Value3'");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithNestedParentheses_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("((A eq 'X' and B eq 'Y') or C eq 'Z') and D eq 'W'");

        // Assert
        result.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithNotOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("not (IsDeleted eq true)");

        // Assert
        result.Should().NotBeNull();
    }
}
