using FluentAssertions;
using KeeperData.Core.Querying.Impl;
using KeeperData.Core.Querying.Models;

namespace KeeperData.Core.Tests.Unit.Querying.Impl;

public class ODataToFilterExpressionTranslatorTests
{
    private readonly ODataToFilterExpressionTranslator _sut = new();

    #region Empty/Null Filter Tests

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Parse_WithEmptyOrNullFilter_ReturnsEmptyFilter(string? filter)
    {
        // Act
        var result = _sut.Parse(filter!);

        // Assert
        result.Should().BeOfType<EmptyFilter>();
    }

    #endregion

    #region Comparison Operator Tests

    [Fact]
    public void Parse_WithEqualOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Name eq 'John'");

        // Assert
        result.Should().BeOfType<ComparisonFilter>();
        var comparison = (ComparisonFilter)result;
        comparison.FieldName.Should().Be("Name");
        comparison.Operator.Should().Be(ComparisonOperator.Equal);
        comparison.Value.Should().Be("John");
    }

    [Fact]
    public void Parse_WithNotEqualOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Status ne 'Active'");

        // Assert
        result.Should().BeOfType<ComparisonFilter>();
        var comparison = (ComparisonFilter)result;
        comparison.FieldName.Should().Be("Status");
        comparison.Operator.Should().Be(ComparisonOperator.NotEqual);
        comparison.Value.Should().Be("Active");
    }

    [Fact]
    public void Parse_WithGreaterThanOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Age gt 18");

        // Assert
        result.Should().BeOfType<ComparisonFilter>();
        var comparison = (ComparisonFilter)result;
        comparison.FieldName.Should().Be("Age");
        comparison.Operator.Should().Be(ComparisonOperator.GreaterThan);
        comparison.Value.Should().Be(18);
    }

    [Fact]
    public void Parse_WithGreaterThanOrEqualOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Score ge 90");

        // Assert
        result.Should().BeOfType<ComparisonFilter>();
        var comparison = (ComparisonFilter)result;
        comparison.FieldName.Should().Be("Score");
        comparison.Operator.Should().Be(ComparisonOperator.GreaterThanOrEqual);
    }

    [Fact]
    public void Parse_WithLessThanOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Price lt 100");

        // Assert
        result.Should().BeOfType<ComparisonFilter>();
        var comparison = (ComparisonFilter)result;
        comparison.FieldName.Should().Be("Price");
        comparison.Operator.Should().Be(ComparisonOperator.LessThan);
    }

    [Fact]
    public void Parse_WithLessThanOrEqualOperator_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Quantity le 50");

        // Assert
        result.Should().BeOfType<ComparisonFilter>();
        var comparison = (ComparisonFilter)result;
        comparison.FieldName.Should().Be("Quantity");
        comparison.Operator.Should().Be(ComparisonOperator.LessThanOrEqual);
    }

    [Fact]
    public void Parse_WithNullValue_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("DeletedAt eq null");

        // Assert
        result.Should().BeOfType<ComparisonFilter>();
        var comparison = (ComparisonFilter)result;
        comparison.FieldName.Should().Be("DeletedAt");
        comparison.Value.Should().BeNull();
    }

    [Fact]
    public void Parse_WithBooleanValue_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("IsActive eq true");

        // Assert
        result.Should().BeOfType<ComparisonFilter>();
        var comparison = (ComparisonFilter)result;
        comparison.FieldName.Should().Be("IsActive");
        comparison.Value.Should().Be(true);
    }

    #endregion

    #region Logical Operator Tests

    [Fact]
    public void Parse_WithAndOperator_ReturnsLogicalFilter()
    {
        // Act
        var result = _sut.Parse("Name eq 'John' and Age gt 18");

        // Assert
        result.Should().BeOfType<LogicalFilter>();
        var logical = (LogicalFilter)result;
        logical.Operator.Should().Be(LogicalOperator.And);
        logical.Filters.Should().HaveCount(2);
        logical.Filters[0].Should().BeOfType<ComparisonFilter>();
        logical.Filters[1].Should().BeOfType<ComparisonFilter>();
    }

    [Fact]
    public void Parse_WithOrOperator_ReturnsLogicalFilter()
    {
        // Act
        var result = _sut.Parse("Status eq 'Active' or Status eq 'Pending'");

        // Assert
        result.Should().BeOfType<LogicalFilter>();
        var logical = (LogicalFilter)result;
        logical.Operator.Should().Be(LogicalOperator.Or);
    }

    [Fact]
    public void Parse_WithNotOperator_ReturnsNotFilter()
    {
        // Act
        var result = _sut.Parse("not (IsDeleted eq true)");

        // Assert
        result.Should().BeOfType<NotFilter>();
        var notFilter = (NotFilter)result;
        notFilter.Filter.Should().BeOfType<ComparisonFilter>();
    }

    [Fact]
    public void Parse_WithNestedLogicalOperators_ReturnsCorrectStructure()
    {
        // Act
        var result = _sut.Parse("(Name eq 'John' and Age gt 18) or Status eq 'VIP'");

        // Assert
        result.Should().BeOfType<LogicalFilter>();
        var outerLogical = (LogicalFilter)result;
        outerLogical.Operator.Should().Be(LogicalOperator.Or);
        outerLogical.Filters[0].Should().BeOfType<LogicalFilter>();
    }

    #endregion

    #region Function Tests

    [Fact]
    public void Parse_WithContainsFunction_ReturnsTextFilter()
    {
        // Act
        var result = _sut.Parse("contains(Name, 'john')");

        // Assert
        result.Should().BeOfType<TextFilter>();
        var textFilter = (TextFilter)result;
        textFilter.FieldName.Should().Be("Name");
        textFilter.Value.Should().Be("john");
        textFilter.Operator.Should().Be(TextOperator.Contains);
    }

    [Fact]
    public void Parse_WithStartsWithFunction_ReturnsTextFilter()
    {
        // Act
        var result = _sut.Parse("startswith(Email, 'admin')");

        // Assert
        result.Should().BeOfType<TextFilter>();
        var textFilter = (TextFilter)result;
        textFilter.FieldName.Should().Be("Email");
        textFilter.Value.Should().Be("admin");
        textFilter.Operator.Should().Be(TextOperator.StartsWith);
    }

    [Fact]
    public void Parse_WithEndsWithFunction_ReturnsTextFilter()
    {
        // Act
        var result = _sut.Parse("endswith(FileName, '.pdf')");

        // Assert
        result.Should().BeOfType<TextFilter>();
        var textFilter = (TextFilter)result;
        textFilter.FieldName.Should().Be("FileName");
        textFilter.Value.Should().Be(".pdf");
        textFilter.Operator.Should().Be(TextOperator.EndsWith);
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public void Parse_WithInvalidFilterSyntax_ThrowsArgumentException()
    {
        // Act
        var act = () => _sut.Parse("invalid filter syntax!!!");

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*Invalid OData filter expression*");
    }

    [Fact]
    public void Parse_WithUnsupportedFunction_ThrowsArgumentException()
    {
        // Act - toupper returns a function call node which is not supported in comparison position
        var act = () => _sut.Parse("toupper(Name) eq 'JOHN'");

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*Expected a property access node*");
    }

    #endregion

    #region Type Conversion Tests

    [Fact]
    public void Parse_WithDecimalValue_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("Price eq 99.99");

        // Assert
        result.Should().BeOfType<ComparisonFilter>();
        var comparison = (ComparisonFilter)result;
        // OData parser may return different numeric types
        comparison.Value.Should().NotBeNull();
    }

    [Fact]
    public void Parse_WithLongValue_ReturnsCorrectFilter()
    {
        // Act
        var result = _sut.Parse("BigNumber gt 9999999999");

        // Assert
        result.Should().BeOfType<ComparisonFilter>();
        var comparison = (ComparisonFilter)result;
        comparison.Value.Should().NotBeNull();
    }

    #endregion
}
