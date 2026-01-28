using FluentAssertions;
using KeeperData.Core.Database;

namespace KeeperData.Core.Tests.Unit.Database;

public class DeleteCollectionResultTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var result = new DeleteCollectionResult
        {
            CollectionName = "test-collection",
            Success = true,
            Message = "Collection deleted successfully"
        };

        result.CollectionName.Should().Be("test-collection");
        result.Success.Should().BeTrue();
        result.Message.Should().Be("Collection deleted successfully");
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var result = new DeleteCollectionResult
        {
            CollectionName = "test",
            Success = true,
            Message = "OK"
        };

        result.Error.Should().BeNull();
        result.OperatedAtUtc.Should().Be(default);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var error = new InvalidOperationException("Test error");
        var operatedAt = DateTime.UtcNow;

        var result = new DeleteCollectionResult
        {
            CollectionName = "users",
            Success = false,
            Message = "Failed to delete collection",
            Error = error,
            OperatedAtUtc = operatedAt
        };

        result.CollectionName.Should().Be("users");
        result.Success.Should().BeFalse();
        result.Message.Should().Be("Failed to delete collection");
        result.Error.Should().Be(error);
        result.OperatedAtUtc.Should().Be(operatedAt);
    }

    [Fact]
    public void Record_SupportsEquality()
    {
        var result1 = new DeleteCollectionResult
        {
            CollectionName = "test",
            Success = true,
            Message = "OK"
        };
        var result2 = new DeleteCollectionResult
        {
            CollectionName = "test",
            Success = true,
            Message = "OK"
        };
        var result3 = new DeleteCollectionResult
        {
            CollectionName = "other",
            Success = true,
            Message = "OK"
        };

        result1.Should().Be(result2);
        result1.Should().NotBe(result3);
    }

    [Fact]
    public void Record_SupportsWithExpression()
    {
        var original = new DeleteCollectionResult
        {
            CollectionName = "test",
            Success = true,
            Message = "OK"
        };
        var modified = original with { Success = false, Message = "Failed" };

        modified.CollectionName.Should().Be("test");
        modified.Success.Should().BeFalse();
        modified.Message.Should().Be("Failed");
    }
}

public class DeleteAllCollectionsResultTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var deletedCollections = new List<string> { "users", "orders", "products" };

        var result = new DeleteAllCollectionsResult
        {
            DeletedCollections = deletedCollections,
            TotalCount = 3,
            Success = true,
            Message = "All collections deleted"
        };

        result.DeletedCollections.Should().BeEquivalentTo(deletedCollections);
        result.TotalCount.Should().Be(3);
        result.Success.Should().BeTrue();
        result.Message.Should().Be("All collections deleted");
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var result = new DeleteAllCollectionsResult
        {
            DeletedCollections = new List<string>(),
            TotalCount = 0,
            Success = true,
            Message = "No collections to delete"
        };

        result.Error.Should().BeNull();
        result.OperatedAtUtc.Should().Be(default);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var error = new Exception("Partial failure");
        var operatedAt = DateTime.UtcNow;
        var deletedCollections = new List<string> { "users" };

        var result = new DeleteAllCollectionsResult
        {
            DeletedCollections = deletedCollections,
            TotalCount = 1,
            Success = false,
            Message = "Partial deletion - some collections failed",
            Error = error,
            OperatedAtUtc = operatedAt
        };

        result.DeletedCollections.Should().ContainSingle().Which.Should().Be("users");
        result.TotalCount.Should().Be(1);
        result.Success.Should().BeFalse();
        result.Message.Should().Contain("Partial deletion");
        result.Error.Should().Be(error);
        result.OperatedAtUtc.Should().Be(operatedAt);
    }

    [Fact]
    public void Record_SupportsEquality()
    {
        var result1 = new DeleteAllCollectionsResult
        {
            DeletedCollections = new List<string> { "test" },
            TotalCount = 1,
            Success = true,
            Message = "OK"
        };
        var result2 = new DeleteAllCollectionsResult
        {
            DeletedCollections = new List<string> { "test" },
            TotalCount = 1,
            Success = true,
            Message = "OK"
        };

        // Note: Record equality compares references for IReadOnlyList, so these won't be equal
        // unless they reference the same list
        result1.Should().NotBe(result2); // Different list instances
    }

    [Fact]
    public void EmptyCollectionsList_CanBeRepresented()
    {
        var result = new DeleteAllCollectionsResult
        {
            DeletedCollections = Array.Empty<string>(),
            TotalCount = 0,
            Success = true,
            Message = "No collections found to delete"
        };

        result.DeletedCollections.Should().BeEmpty();
        result.TotalCount.Should().Be(0);
    }
}
