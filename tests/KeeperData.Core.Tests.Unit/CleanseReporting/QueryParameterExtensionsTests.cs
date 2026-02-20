using FluentAssertions;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports;

namespace KeeperData.Core.Tests.Unit.CleanseReporting;

public class QueryParameterExtensionsTests
{
    [Fact]
    public void GenerateCacheKey_SameParameters_ShouldReturnSameKey()
    {
        var p1 = new QueryParameters { CollectionName = "test", Skip = 0, Top = 10 };
        var p2 = new QueryParameters { CollectionName = "test", Skip = 0, Top = 10 };

        p1.GenerateCacheKey().Should().Be(p2.GenerateCacheKey());
    }

    [Fact]
    public void GenerateCacheKey_DifferentCollections_ShouldReturnDifferentKeys()
    {
        var p1 = new QueryParameters { CollectionName = "col_a", Skip = 0, Top = 10 };
        var p2 = new QueryParameters { CollectionName = "col_b", Skip = 0, Top = 10 };

        p1.GenerateCacheKey().Should().NotBe(p2.GenerateCacheKey());
    }

    [Fact]
    public void GenerateCacheKey_DifferentSkip_ShouldReturnDifferentKeys()
    {
        var p1 = new QueryParameters { CollectionName = "test", Skip = 0, Top = 10 };
        var p2 = new QueryParameters { CollectionName = "test", Skip = 10, Top = 10 };

        p1.GenerateCacheKey().Should().NotBe(p2.GenerateCacheKey());
    }

    [Fact]
    public void GenerateCacheKey_WithFieldsToSelect_ShouldIncludeInHash()
    {
        var p1 = new QueryParameters { CollectionName = "test", Skip = 0, Top = 10, FieldsToSelect = ["A", "B"] };
        var p2 = new QueryParameters { CollectionName = "test", Skip = 0, Top = 10, FieldsToSelect = ["A", "C"] };

        p1.GenerateCacheKey().Should().NotBe(p2.GenerateCacheKey());
    }

    [Fact]
    public void GenerateCacheKey_ShouldReturnHexString()
    {
        var p = new QueryParameters { CollectionName = "test", Skip = 0, Top = 10 };

        var key = p.GenerateCacheKey();

        key.Should().MatchRegex("^[0-9A-F]+$");
    }
}
