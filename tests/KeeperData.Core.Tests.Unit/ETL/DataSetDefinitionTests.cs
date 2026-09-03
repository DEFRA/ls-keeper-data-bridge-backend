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

    [Fact]
    public void DataSetDefinition_ShouldDefaultToDiscoveryByPrefixWithNoAuditLane()
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
        definition.SourceKeyPattern.Should().BeNull();
        definition.BaselineKeyPattern.Should().BeNull();
        definition.Audit.Should().BeNull();
        definition.ExcludedColumns.Should().BeEmpty();
    }

    [Fact]
    public void DataSetDefinition_CanBeCreatedWithGlobDiscoveryAndAnAuditLane()
    {
        // Act
        var definition = new DataSetDefinition(
            "cts_location_identifiers",
            "cads/cts/",
            new[] { "LID_ID" },
            "LID_AUD_TYPE",
            Array.Empty<string>(),
            SourceKeyPattern: "cads/cts/*/*CT_LOCATION_IDENTIFIERS*.csv",
            BaselineKeyPattern: "cads/cts/bulk/*CT_LOCATION_IDENTIFIERS*.csv",
            Audit: new AuditColumns("LID_AUD_ID", "LID_AUD_DATETIME")
        )
        {
            ExcludedColumns = ["LID_AUD_ID", "LID_AUD_TYPE", "LID_AUD_DATETIME"]
        };

        // Assert
        definition.SourceKeyPattern.Should().Be("cads/cts/*/*CT_LOCATION_IDENTIFIERS*.csv");
        definition.BaselineKeyPattern.Should().Be("cads/cts/bulk/*CT_LOCATION_IDENTIFIERS*.csv");
        definition.Audit.Should().Be(new AuditColumns("LID_AUD_ID", "LID_AUD_DATETIME"));
        definition.ChangeTypeHeaderName.Should().Be("LID_AUD_TYPE", "the audit columns do not redeclare the change type");
        definition.ExcludedColumns.Should().Equal("LID_AUD_ID", "LID_AUD_TYPE", "LID_AUD_DATETIME");
    }

    [Fact]
    public void DataSetDefinitions_ShouldLeaveCtsLocationIdentifiersUnpopulatedUntilItIsWiredUp()
    {
        // Act
        var definitions = StandardDataSetDefinitionsBuilder.Build();

        // Assert
        definitions.CtsLocationIdentifiers.Should().BeNull();
        definitions.All.Should().HaveCount(12);
    }
}
