using FluentAssertions;
using KeeperData.Core.Querying.Models;

namespace KeeperData.Core.Tests.Unit.Querying.Models;

public class FilterExpressionTests
{
    #region ComparisonFilter Tests

    [Fact]
    public void Equal_CreatesCorrectFilter()
    {
        var filter = FilterExpression.Equal("name", "John") as ComparisonFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("name");
        filter.Operator.Should().Be(ComparisonOperator.Equal);
        filter.Value.Should().Be("John");
    }

    [Fact]
    public void NotEqual_CreatesCorrectFilter()
    {
        var filter = FilterExpression.NotEqual("status", "active") as ComparisonFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("status");
        filter.Operator.Should().Be(ComparisonOperator.NotEqual);
        filter.Value.Should().Be("active");
    }

    [Fact]
    public void GreaterThan_CreatesCorrectFilter()
    {
        var filter = FilterExpression.GreaterThan("age", 18) as ComparisonFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("age");
        filter.Operator.Should().Be(ComparisonOperator.GreaterThan);
        filter.Value.Should().Be(18);
    }

    [Fact]
    public void GreaterThanOrEqual_CreatesCorrectFilter()
    {
        var filter = FilterExpression.GreaterThanOrEqual("age", 21) as ComparisonFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("age");
        filter.Operator.Should().Be(ComparisonOperator.GreaterThanOrEqual);
        filter.Value.Should().Be(21);
    }

    [Fact]
    public void LessThan_CreatesCorrectFilter()
    {
        var filter = FilterExpression.LessThan("price", 100) as ComparisonFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("price");
        filter.Operator.Should().Be(ComparisonOperator.LessThan);
        filter.Value.Should().Be(100);
    }

    [Fact]
    public void LessThanOrEqual_CreatesCorrectFilter()
    {
        var filter = FilterExpression.LessThanOrEqual("price", 50) as ComparisonFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("price");
        filter.Operator.Should().Be(ComparisonOperator.LessThanOrEqual);
        filter.Value.Should().Be(50);
    }

    [Fact]
    public void ComparisonFilter_WithNullFieldName_ThrowsArgumentNullException()
    {
        var act = () => new ComparisonFilter(null!, ComparisonOperator.Equal, "value");

        act.Should().Throw<ArgumentNullException>();
    }

    [Theory]
    [InlineData(ComparisonOperator.Equal, "==")]
    [InlineData(ComparisonOperator.NotEqual, "!=")]
    [InlineData(ComparisonOperator.GreaterThan, ">")]
    [InlineData(ComparisonOperator.GreaterThanOrEqual, ">=")]
    [InlineData(ComparisonOperator.LessThan, "<")]
    [InlineData(ComparisonOperator.LessThanOrEqual, "<=")]
    public void ComparisonFilter_ToString_FormatsCorrectly(ComparisonOperator op, string expectedOp)
    {
        var filter = new ComparisonFilter("field", op, 42);

        var result = filter.ToString();

        result.Should().Contain("field");
        result.Should().Contain(expectedOp);
        result.Should().Contain("42");
    }

    [Fact]
    public void ComparisonFilter_ToString_FormatsNullValue()
    {
        var filter = new ComparisonFilter("field", ComparisonOperator.Equal, null);

        filter.ToString().Should().Contain("null");
    }

    [Fact]
    public void ComparisonFilter_ToString_FormatsStringValue()
    {
        var filter = new ComparisonFilter("name", ComparisonOperator.Equal, "John");

        filter.ToString().Should().Contain("'John'");
    }

    [Fact]
    public void ComparisonFilter_ToString_FormatsBoolValue()
    {
        var filter = new ComparisonFilter("active", ComparisonOperator.Equal, true);

        filter.ToString().Should().Contain("true");
    }

    [Fact]
    public void ComparisonFilter_ToString_FormatsDateTimeValue()
    {
        var date = new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Utc);
        var filter = new ComparisonFilter("createdAt", ComparisonOperator.Equal, date);

        filter.ToString().Should().Contain("2024-06-15");
    }

    #endregion

    #region InFilter Tests

    [Fact]
    public void In_CreatesCorrectFilter()
    {
        var values = new object[] { 1, 2, 3 };
        var filter = FilterExpression.In("id", values) as InFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("id");
        filter.Values.Should().BeEquivalentTo(values);
    }

    [Fact]
    public void InFilter_WithNullFieldName_ThrowsArgumentNullException()
    {
        var act = () => new InFilter(null!, new object[] { 1, 2 });

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void InFilter_WithNullValues_ThrowsArgumentNullException()
    {
        var act = () => new InFilter("field", null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void InFilter_ToString_FormatsCorrectly()
    {
        var filter = new InFilter("status", new object[] { "active", "pending" });

        var result = filter.ToString();

        result.Should().Contain("status");
        result.Should().Contain("in");
        result.Should().Contain("'active'");
        result.Should().Contain("'pending'");
    }

    #endregion

    #region TextFilter Tests

    [Fact]
    public void Contains_CreatesCorrectFilter()
    {
        var filter = FilterExpression.Contains("name", "John") as TextFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("name");
        filter.Operator.Should().Be(TextOperator.Contains);
        filter.Value.Should().Be("John");
        filter.CaseSensitive.Should().BeFalse();
    }

    [Fact]
    public void StartsWith_CreatesCorrectFilter()
    {
        var filter = FilterExpression.StartsWith("name", "J", true) as TextFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("name");
        filter.Operator.Should().Be(TextOperator.StartsWith);
        filter.Value.Should().Be("J");
        filter.CaseSensitive.Should().BeTrue();
    }

    [Fact]
    public void EndsWith_CreatesCorrectFilter()
    {
        var filter = FilterExpression.EndsWith("email", ".com") as TextFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("email");
        filter.Operator.Should().Be(TextOperator.EndsWith);
        filter.Value.Should().Be(".com");
    }

    [Fact]
    public void TextFilter_WithNullFieldName_ThrowsArgumentNullException()
    {
        var act = () => new TextFilter(null!, TextOperator.Contains, "value", false);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void TextFilter_WithNullValue_ThrowsArgumentNullException()
    {
        var act = () => new TextFilter("field", TextOperator.Contains, null!, false);

        act.Should().Throw<ArgumentNullException>();
    }

    [Theory]
    [InlineData(TextOperator.Contains, "contains")]
    [InlineData(TextOperator.StartsWith, "startsWith")]
    [InlineData(TextOperator.EndsWith, "endsWith")]
    public void TextFilter_ToString_FormatsCorrectly(TextOperator op, string expectedOp)
    {
        var filter = new TextFilter("field", op, "value", false);

        var result = filter.ToString();

        result.Should().Contain("field");
        result.Should().Contain(expectedOp);
        result.Should().Contain("'value'");
        result.Should().Contain("case-insensitive");
    }

    [Fact]
    public void TextFilter_ToString_CaseSensitive_OmitsSuffix()
    {
        var filter = new TextFilter("field", TextOperator.Contains, "value", true);

        var result = filter.ToString();

        result.Should().NotContain("case-insensitive");
    }

    #endregion

    #region RegexFilter Tests

    [Fact]
    public void Regex_CreatesCorrectFilter()
    {
        var filter = FilterExpression.Regex("code", "^ABC") as RegexFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("code");
        filter.Pattern.Should().Be("^ABC");
        filter.CaseSensitive.Should().BeFalse();
    }

    [Fact]
    public void RegexFilter_WithNullFieldName_ThrowsArgumentNullException()
    {
        var act = () => new RegexFilter(null!, "pattern", false);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void RegexFilter_WithNullPattern_ThrowsArgumentNullException()
    {
        var act = () => new RegexFilter("field", null!, false);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void RegexFilter_ToString_FormatsCorrectly()
    {
        var filter = new RegexFilter("code", "^ABC.*", false);

        var result = filter.ToString();

        result.Should().Contain("code");
        result.Should().Contain("matches");
        result.Should().Contain("/^ABC.*/");
    }

    #endregion

    #region ExistsFilter Tests

    [Fact]
    public void Exists_CreatesCorrectFilter()
    {
        var filter = FilterExpression.Exists("email") as ExistsFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("email");
        filter.ShouldExist.Should().BeTrue();
    }

    [Fact]
    public void NotExists_CreatesCorrectFilter()
    {
        var filter = FilterExpression.NotExists("deletedAt") as ExistsFilter;

        filter.Should().NotBeNull();
        filter!.FieldName.Should().Be("deletedAt");
        filter.ShouldExist.Should().BeFalse();
    }

    [Fact]
    public void ExistsFilter_WithNullFieldName_ThrowsArgumentNullException()
    {
        var act = () => new ExistsFilter(null!, true);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void ExistsFilter_ToString_FormatsExistsCorrectly()
    {
        var filter = new ExistsFilter("field", true);

        filter.ToString().Should().Be("field exists");
    }

    [Fact]
    public void ExistsFilter_ToString_FormatsNotExistsCorrectly()
    {
        var filter = new ExistsFilter("field", false);

        filter.ToString().Should().Be("field not exists");
    }

    #endregion

    #region LogicalFilter Tests

    [Fact]
    public void And_CreatesCorrectFilter()
    {
        var filter1 = FilterExpression.Equal("a", 1);
        var filter2 = FilterExpression.Equal("b", 2);
        var andFilter = FilterExpression.And(filter1, filter2) as LogicalFilter;

        andFilter.Should().NotBeNull();
        andFilter!.Operator.Should().Be(LogicalOperator.And);
        andFilter.Filters.Should().HaveCount(2);
    }

    [Fact]
    public void Or_CreatesCorrectFilter()
    {
        var filter1 = FilterExpression.Equal("status", "active");
        var filter2 = FilterExpression.Equal("status", "pending");
        var orFilter = FilterExpression.Or(filter1, filter2) as LogicalFilter;

        orFilter.Should().NotBeNull();
        orFilter!.Operator.Should().Be(LogicalOperator.Or);
        orFilter.Filters.Should().HaveCount(2);
    }

    [Fact]
    public void LogicalFilter_WithNullFilters_ThrowsArgumentNullException()
    {
        var act = () => new LogicalFilter(LogicalOperator.And, null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void LogicalFilter_WithEmptyFilters_ThrowsArgumentException()
    {
        var act = () => new LogicalFilter(LogicalOperator.And);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void LogicalFilter_ToString_FormatsAndCorrectly()
    {
        var filter = new LogicalFilter(LogicalOperator.And,
            FilterExpression.Equal("a", 1),
            FilterExpression.Equal("b", 2));

        var result = filter.ToString();

        result.Should().Contain(" AND ");
    }

    [Fact]
    public void LogicalFilter_ToString_FormatsOrCorrectly()
    {
        var filter = new LogicalFilter(LogicalOperator.Or,
            FilterExpression.Equal("a", 1),
            FilterExpression.Equal("b", 2));

        var result = filter.ToString();

        result.Should().Contain(" OR ");
    }

    [Fact]
    public void LogicalFilter_ToString_WrapsNestedLogicalFiltersInParentheses()
    {
        var innerFilter = new LogicalFilter(LogicalOperator.Or,
            FilterExpression.Equal("a", 1),
            FilterExpression.Equal("b", 2));
        var outerFilter = new LogicalFilter(LogicalOperator.And,
            innerFilter,
            FilterExpression.Equal("c", 3));

        var result = outerFilter.ToString();

        result.Should().Contain("(");
        result.Should().Contain(")");
    }

    #endregion

    #region NotFilter Tests

    [Fact]
    public void Not_CreatesCorrectFilter()
    {
        var innerFilter = FilterExpression.Equal("active", true);
        var notFilter = FilterExpression.Not(innerFilter) as NotFilter;

        notFilter.Should().NotBeNull();
        notFilter!.Filter.Should().Be(innerFilter);
    }

    [Fact]
    public void NotFilter_WithNullFilter_ThrowsArgumentNullException()
    {
        var act = () => new NotFilter(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void NotFilter_ToString_FormatsSimpleFilterCorrectly()
    {
        var filter = new NotFilter(FilterExpression.Equal("active", true));

        var result = filter.ToString();

        result.Should().StartWith("NOT ");
        result.Should().NotContain("(");
    }

    [Fact]
    public void NotFilter_ToString_WrapsLogicalFilterInParentheses()
    {
        var logicalFilter = new LogicalFilter(LogicalOperator.Or,
            FilterExpression.Equal("a", 1),
            FilterExpression.Equal("b", 2));
        var filter = new NotFilter(logicalFilter);

        var result = filter.ToString();

        result.Should().StartWith("NOT (");
        result.Should().EndWith(")");
    }

    #endregion

    #region EmptyFilter Tests

    [Fact]
    public void Empty_CreatesEmptyFilter()
    {
        var filter = FilterExpression.Empty();

        filter.Should().BeOfType<EmptyFilter>();
    }

    [Fact]
    public void EmptyFilter_ToString_ReturnsEmpty()
    {
        var filter = new EmptyFilter();

        filter.ToString().Should().Be("(empty)");
    }

    #endregion
}
