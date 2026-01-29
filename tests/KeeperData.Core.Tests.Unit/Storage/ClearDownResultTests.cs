using FluentAssertions;
using KeeperData.Core.Storage;

namespace KeeperData.Core.Tests.Unit.Storage;

public class ClearDownResultTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var deletedKeys = new List<string> { "file1.csv", "file2.csv" };

        var result = new ClearDownResult
        {
            DeletedKeys = deletedKeys,
            TotalDeleted = 2
        };

        result.DeletedKeys.Should().BeEquivalentTo(deletedKeys);
        result.TotalDeleted.Should().Be(2);
    }

    [Fact]
    public void EmptyResult_CanBeRepresented()
    {
        var result = new ClearDownResult
        {
            DeletedKeys = Array.Empty<string>(),
            TotalDeleted = 0
        };

        result.DeletedKeys.Should().BeEmpty();
        result.TotalDeleted.Should().Be(0);
    }

    [Fact]
    public void Record_IsSealed()
    {
        typeof(ClearDownResult).IsSealed.Should().BeTrue();
    }

    [Fact]
    public void Record_SupportsWithExpression()
    {
        var original = new ClearDownResult
        {
            DeletedKeys = new List<string> { "file1.csv" },
            TotalDeleted = 1
        };

        var modified = original with { TotalDeleted = 5 };

        modified.DeletedKeys.Should().BeEquivalentTo(original.DeletedKeys);
        modified.TotalDeleted.Should().Be(5);
    }

    [Fact]
    public void MultipleKeys_AreStoredCorrectly()
    {
        var keys = new List<string>
        {
            "folder/file1.csv",
            "folder/file2.csv",
            "folder/subfolder/file3.csv"
        };

        var result = new ClearDownResult
        {
            DeletedKeys = keys,
            TotalDeleted = 3
        };

        result.DeletedKeys.Should().HaveCount(3);
        result.DeletedKeys.Should().Contain("folder/file1.csv");
        result.DeletedKeys.Should().Contain("folder/subfolder/file3.csv");
    }
}
