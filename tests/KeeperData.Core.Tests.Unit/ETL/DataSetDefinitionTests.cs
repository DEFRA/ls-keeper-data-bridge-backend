using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using Xunit;

namespace KeeperData.Core.Tests.Unit.ETL;

[Trait("Category", "Unit")]
public class DataSetDefinitionTests
{
    [Fact]
    public void DataSetDefinition_ShouldDefaultToSimplePsvFormat()
    {
        // Act
        var definition = new DataSetDefinition(
            "test_dataset",
            "PREFIX_{0}",
            new[] { "ID" },
            "ChangeType",
            Array.Empty<string>()
        );

        // Assert
        definition.Format.Should().Be(FileFormat.SimplePsv);
    }

    [Fact]
    public void DataSetDefinition_CanBeCreatedWithHcdtFormat()
    {
        // Act
        var definition = new DataSetDefinition(
            "test_dataset",
            "PREFIX_{0}",
            new[] { "ID" },
            "ChangeType",
            Array.Empty<string>(),
            Format: FileFormat.Hcdt
        );

        // Assert
        definition.Format.Should().Be(FileFormat.Hcdt);
    }
}
