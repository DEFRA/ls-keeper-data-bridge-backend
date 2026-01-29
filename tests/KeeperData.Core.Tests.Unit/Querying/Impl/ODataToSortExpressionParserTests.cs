using FluentAssertions;
using KeeperData.Core.Querying.Impl;
using KeeperData.Core.Querying.Models;

namespace KeeperData.Core.Tests.Unit.Querying.Impl;

public class ODataToSortExpressionParserTests
{
    private readonly ODataToSortExpressionParser _sut = new();

    #region Empty/Null Expression Tests

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Parse_WithEmptyOrNullExpression_ThrowsArgumentException(string? expression)
    {
        // Act
        var act = () => _sut.Parse(expression!);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*OrderBy expression cannot be empty*");
    }

    #endregion

    #region Single Field Tests

    [Fact]
    public void Parse_WithSingleFieldAscending_ReturnsSingleFieldSort()
    {
        // Act
        var result = _sut.Parse("Name asc");

        // Assert
        result.Should().BeOfType<SingleFieldSort>();
        var sort = (SingleFieldSort)result;
        sort.FieldName.Should().Be("Name");
        sort.Direction.Should().Be(SortDirection.Ascending);
    }

    [Fact]
    public void Parse_WithSingleFieldDescending_ReturnsSingleFieldSort()
    {
        // Act
        var result = _sut.Parse("CreatedDate desc");

        // Assert
        result.Should().BeOfType<SingleFieldSort>();
        var sort = (SingleFieldSort)result;
        sort.FieldName.Should().Be("CreatedDate");
        sort.Direction.Should().Be(SortDirection.Descending);
    }

    [Fact]
    public void Parse_WithSingleFieldNoDirection_DefaultsToAscending()
    {
        // Act
        var result = _sut.Parse("Name");

        // Assert
        result.Should().BeOfType<SingleFieldSort>();
        var sort = (SingleFieldSort)result;
        sort.FieldName.Should().Be("Name");
        sort.Direction.Should().Be(SortDirection.Ascending);
    }

    [Theory]
    [InlineData("Name ASC")]
    [InlineData("Name Asc")]
    [InlineData("Name asc")]
    public void Parse_WithMixedCaseDirection_ParsesCorrectly(string expression)
    {
        // Act
        var result = _sut.Parse(expression);

        // Assert
        result.Should().BeOfType<SingleFieldSort>();
        var sort = (SingleFieldSort)result;
        sort.Direction.Should().Be(SortDirection.Ascending);
    }

    [Theory]
    [InlineData("Age DESC")]
    [InlineData("Age Desc")]
    [InlineData("Age desc")]
    public void Parse_WithMixedCaseDescending_ParsesCorrectly(string expression)
    {
        // Act
        var result = _sut.Parse(expression);

        // Assert
        result.Should().BeOfType<SingleFieldSort>();
        var sort = (SingleFieldSort)result;
        sort.Direction.Should().Be(SortDirection.Descending);
    }

    #endregion

    #region Multiple Fields Tests

    [Fact]
    public void Parse_WithTwoFields_ReturnsCompoundSort()
    {
        // Act
        var result = _sut.Parse("LastName asc, FirstName asc");

        // Assert
        result.Should().BeOfType<CompoundSort>();
        var compound = (CompoundSort)result;
        compound.Sorts.Should().HaveCount(2);
        
        var sort0 = compound.Sorts[0].Should().BeOfType<SingleFieldSort>().Subject;
        var sort1 = compound.Sorts[1].Should().BeOfType<SingleFieldSort>().Subject;
        
        sort0.FieldName.Should().Be("LastName");
        sort1.FieldName.Should().Be("FirstName");
    }

    [Fact]
    public void Parse_WithMixedDirections_PreservesDirections()
    {
        // Act
        var result = _sut.Parse("Priority desc, CreatedAt asc");

        // Assert
        result.Should().BeOfType<CompoundSort>();
        var compound = (CompoundSort)result;
        
        var sort0 = compound.Sorts[0].Should().BeOfType<SingleFieldSort>().Subject;
        var sort1 = compound.Sorts[1].Should().BeOfType<SingleFieldSort>().Subject;
        
        sort0.Direction.Should().Be(SortDirection.Descending);
        sort1.Direction.Should().Be(SortDirection.Ascending);
    }

    [Fact]
    public void Parse_WithMultipleFields_AllAscending()
    {
        // Act
        var result = _sut.Parse("Field1, Field2, Field3");

        // Assert
        result.Should().BeOfType<CompoundSort>();
        var compound = (CompoundSort)result;
        compound.Sorts.Should().HaveCount(3);
        compound.Sorts.Should().AllSatisfy(s =>
        {
            var singleSort = s.Should().BeOfType<SingleFieldSort>().Subject;
            singleSort.Direction.Should().Be(SortDirection.Ascending);
        });
    }

    #endregion

    #region Whitespace Handling Tests

    [Fact]
    public void Parse_WithExtraWhitespace_TrimsCorrectly()
    {
        // Act
        var result = _sut.Parse("  Name   asc  ,   Age   desc  ");

        // Assert
        result.Should().BeOfType<CompoundSort>();
        var compound = (CompoundSort)result;
        
        var sort0 = compound.Sorts[0].Should().BeOfType<SingleFieldSort>().Subject;
        var sort1 = compound.Sorts[1].Should().BeOfType<SingleFieldSort>().Subject;
        
        sort0.FieldName.Should().Be("Name");
        sort1.FieldName.Should().Be("Age");
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public void Parse_WithInvalidDirection_ThrowsArgumentException()
    {
        // Act
        var act = () => _sut.Parse("Name invalid");

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithMessage("*Invalid orderBy clause*");
    }

    #endregion
}
