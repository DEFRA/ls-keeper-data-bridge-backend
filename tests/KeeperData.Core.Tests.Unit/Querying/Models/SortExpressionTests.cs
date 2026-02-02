using FluentAssertions;
using KeeperData.Core.Querying.Models;

namespace KeeperData.Core.Tests.Unit.Querying.Models;

public class SortExpressionTests
{
    [Fact]
    public void Ascending_CreatesSingleFieldSortWithAscendingDirection()
    {
        var sort = SortExpression.Ascending("name") as SingleFieldSort;

        sort.Should().NotBeNull();
        sort!.FieldName.Should().Be("name");
        sort.Direction.Should().Be(SortDirection.Ascending);
    }

    [Fact]
    public void Descending_CreatesSingleFieldSortWithDescendingDirection()
    {
        var sort = SortExpression.Descending("createdAt") as SingleFieldSort;

        sort.Should().NotBeNull();
        sort!.FieldName.Should().Be("createdAt");
        sort.Direction.Should().Be(SortDirection.Descending);
    }

    [Fact]
    public void SingleFieldSort_WithNullFieldName_ThrowsArgumentNullException()
    {
        var act = () => new SingleFieldSort(null!, SortDirection.Ascending);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Combine_CreatesCompoundSort()
    {
        var sort1 = SortExpression.Ascending("lastName");
        var sort2 = SortExpression.Ascending("firstName");
        var combined = SortExpression.Combine(sort1, sort2) as CompoundSort;

        combined.Should().NotBeNull();
        combined!.Sorts.Should().HaveCount(2);
        combined.Sorts[0].Should().Be(sort1);
        combined.Sorts[1].Should().Be(sort2);
    }

    [Fact]
    public void CompoundSort_WithNullSorts_ThrowsArgumentNullException()
    {
        var act = () => new CompoundSort(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void CompoundSort_WithEmptySorts_ThrowsArgumentException()
    {
        var act = () => new CompoundSort();

        act.Should().Throw<ArgumentException>()
            .WithMessage("*At least one sort expression*");
    }

    [Fact]
    public void Combine_WithMultipleSorts_PreservesOrder()
    {
        var sort1 = SortExpression.Descending("priority");
        var sort2 = SortExpression.Ascending("createdAt");
        var sort3 = SortExpression.Ascending("id");

        var combined = SortExpression.Combine(sort1, sort2, sort3) as CompoundSort;

        combined.Should().NotBeNull();
        combined!.Sorts.Should().HaveCount(3);
        
        var first = combined.Sorts[0] as SingleFieldSort;
        first!.FieldName.Should().Be("priority");
        first.Direction.Should().Be(SortDirection.Descending);

        var second = combined.Sorts[1] as SingleFieldSort;
        second!.FieldName.Should().Be("createdAt");
        second.Direction.Should().Be(SortDirection.Ascending);
    }
}
