using FluentAssertions;
using KeeperData.Core.Querying.Models;

namespace KeeperData.Core.Tests.Unit.Querying.Models;

public class QueryParametersTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var parameters = new QueryParameters
        {
            CollectionName = "test-collection"
        };

        parameters.CollectionName.Should().Be("test-collection");
    }

    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var parameters = new QueryParameters
        {
            CollectionName = "test"
        };

        parameters.Filter.Should().BeNull();
        parameters.Sort.Should().BeNull();
        parameters.FieldsToSelect.Should().BeNull();
        parameters.Skip.Should().Be(0);
        parameters.Top.Should().Be(0);
        parameters.IncludeCount.Should().BeTrue();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var filter = FilterExpression.Equal("status", "active");
        var sort = SortExpression.Ascending("name");
        var fields = new List<string> { "id", "name", "status" };

        var parameters = new QueryParameters
        {
            CollectionName = "users",
            Filter = filter,
            Sort = sort,
            FieldsToSelect = fields,
            Skip = 10,
            Top = 25,
            IncludeCount = false
        };

        parameters.CollectionName.Should().Be("users");
        parameters.Filter.Should().Be(filter);
        parameters.Sort.Should().Be(sort);
        parameters.FieldsToSelect.Should().BeEquivalentTo(fields);
        parameters.Skip.Should().Be(10);
        parameters.Top.Should().Be(25);
        parameters.IncludeCount.Should().BeFalse();
    }
}

public class QueryResultTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var result = new QueryResult
        {
            CollectionName = "test-collection",
            Data = new List<Dictionary<string, object?>>(),
            Count = 0
        };

        result.CollectionName.Should().Be("test-collection");
        result.Data.Should().BeEmpty();
        result.Count.Should().Be(0);
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var result = new QueryResult
        {
            CollectionName = "test",
            Data = [],
            Count = 0
        };

        result.TotalCount.Should().BeNull();
        result.Skip.Should().BeNull();
        result.Top.Should().BeNull();
        result.Filter.Should().BeNull();
        result.OrderBy.Should().BeNull();
        result.Select.Should().BeNull();
        result.ExecutedAtUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var data = new List<Dictionary<string, object?>>
        {
            new() { { "id", 1 }, { "name", "John" } },
            new() { { "id", 2 }, { "name", "Jane" } }
        };
        var executedAt = DateTime.UtcNow.AddMinutes(-5);

        var result = new QueryResult
        {
            CollectionName = "users",
            Data = data,
            Count = 2,
            TotalCount = 100,
            Skip = 0,
            Top = 10,
            Filter = "status eq 'active'",
            OrderBy = "name asc",
            Select = "id,name",
            ExecutedAtUtc = executedAt
        };

        result.CollectionName.Should().Be("users");
        result.Data.Should().HaveCount(2);
        result.Count.Should().Be(2);
        result.TotalCount.Should().Be(100);
        result.Skip.Should().Be(0);
        result.Top.Should().Be(10);
        result.Filter.Should().Be("status eq 'active'");
        result.OrderBy.Should().Be("name asc");
        result.Select.Should().Be("id,name");
        result.ExecutedAtUtc.Should().Be(executedAt);
    }

    [Fact]
    public void Combine_WithNullOrEmptyArray_ReturnsEmptyResult()
    {
        var result = QueryResult.Combine(null!);

        result.Should().NotBeNull();
        result.CollectionName.Should().BeEmpty();
        result.Data.Should().BeEmpty();
        result.Count.Should().Be(0);
    }

    [Fact]
    public void Combine_WithEmptyArray_ReturnsEmptyResult()
    {
        var result = QueryResult.Combine();

        result.Should().NotBeNull();
        result.CollectionName.Should().BeEmpty();
        result.Data.Should().BeEmpty();
        result.Count.Should().Be(0);
    }

    [Fact]
    public void Combine_WithSingleResult_ReturnsSameData()
    {
        var data = new List<Dictionary<string, object?>>
        {
            new() { { "id", 1 } }
        };
        var original = new QueryResult
        {
            CollectionName = "test",
            Data = data,
            Count = 1,
            TotalCount = 10
        };

        var result = QueryResult.Combine(original);

        result.CollectionName.Should().Be("test");
        result.Data.Should().HaveCount(1);
        result.Count.Should().Be(1);
        result.TotalCount.Should().Be(10);
    }

    [Fact]
    public void Combine_WithMultipleResults_CombinesData()
    {
        var result1 = new QueryResult
        {
            CollectionName = "users",
            Data = new List<Dictionary<string, object?>>
            {
                new() { { "id", 1 }, { "name", "John" } }
            },
            Count = 1,
            TotalCount = 50
        };

        var result2 = new QueryResult
        {
            CollectionName = "users",
            Data = new List<Dictionary<string, object?>>
            {
                new() { { "id", 2 }, { "name", "Jane" } },
                new() { { "id", 3 }, { "name", "Bob" } }
            },
            Count = 2,
            TotalCount = 50
        };

        var combined = QueryResult.Combine(result1, result2);

        combined.CollectionName.Should().Be("users");
        combined.Data.Should().HaveCount(3);
        combined.Count.Should().Be(3);
        combined.TotalCount.Should().Be(100);
    }

    [Fact]
    public void Combine_UsesFirstResultCollectionName()
    {
        var result1 = new QueryResult { CollectionName = "first", Data = [], Count = 0 };
        var result2 = new QueryResult { CollectionName = "second", Data = [], Count = 0 };

        var combined = QueryResult.Combine(result1, result2);

        combined.CollectionName.Should().Be("first");
    }

    [Fact]
    public void Combine_SetsNewExecutedAtUtc()
    {
        var oldTime = DateTime.UtcNow.AddHours(-1);
        var result1 = new QueryResult
        {
            CollectionName = "test",
            Data = [],
            Count = 0,
            ExecutedAtUtc = oldTime
        };

        var combined = QueryResult.Combine(result1);

        combined.ExecutedAtUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
    }
}
