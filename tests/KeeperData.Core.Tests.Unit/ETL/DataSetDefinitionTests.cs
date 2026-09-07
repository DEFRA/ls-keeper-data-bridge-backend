using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using Xunit;

namespace KeeperData.Core.Tests.Unit.ETL;

[Trait("Category", "Unit")]
public class DataSetDefinitionTests
{
    private static readonly string[] IdPrimaryKey = ["ID"];
    private static readonly string[] LidIdPrimaryKey = ["LID_ID"];
    private static readonly string[] NoExcludedColumns = [];

    [Fact]
    public void DataSetDefinition_ShouldDefaultToSimplePsvFormat()
    {
        // Act
        var definition = new DataSetDefinition(
            "test_dataset",
            "PREFIX_{0}",
            IdPrimaryKey,
            "ChangeType",
            NoExcludedColumns
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
            IdPrimaryKey,
            "ChangeType",
            NoExcludedColumns,
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
            IdPrimaryKey,
            "ChangeType",
            NoExcludedColumns
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
            LidIdPrimaryKey,
            "LID_AUD_TYPE",
            NoExcludedColumns,
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
    public void DataSetDefinitions_ShouldRegisterCtsLocationIdentifiersAlongsideTheLitprdDatasets()
    {
        // Act
        var definitions = StandardDataSetDefinitionsBuilder.Build();

        // Assert
        definitions.All.Should().HaveCount(13);
        definitions.All.Should().Contain(definitions.CtsLocationIdentifiers!);
    }

    [Fact]
    public void CtsLocationIdentifiers_ShouldDescribeTheFeedItIsDiscoveredAndMergedBy()
    {
        // Act
        var definition = StandardDataSetDefinitionsBuilder.Build().CtsLocationIdentifiers;

        // Assert
        definition.Should().NotBeNull();
        definition!.Name.Should().Be("cts_location_identifiers");
        definition.PrimaryKeyHeaderNames.Should().Equal("LID_ID");
        definition.ChangeTypeHeaderName.Should().Be("LID_AUD_TYPE");
        definition.Accumulators.Should().BeEmpty();
        definition.DateTimePattern.Should().Be("yyyy-MM-dd-HHmmss");
        definition.Format.Should().Be(FileFormat.SimplePsv);
        definition.PasswordDerivation.Should().Be(PasswordDerivationPolicy.CtsDerived);
        definition.SourceKeyPattern.Should().Be("cads/cts/{bulk,daily}/*CT_LOCATION_IDENTIFIERS*");
        definition.BaselineKeyPattern.Should().Be("cads/cts/bulk/{CT_LOCATION_IDENTIFIERS_*,*_BULK_*_CT_LOCATION_IDENTIFIERS_*}");
        definition.Audit.Should().Be(new AuditColumns("LID_AUD_ID", "LID_AUD_DATETIME"));
        definition.ExcludedColumns.Should().Equal(
            "LID_AUD_ID", "LID_AUD_TYPE", "LID_AUD_DATETIME", "RECORD_TYPE", "RECORD_COUNT");
    }

    /// <summary>The two lanes are listed separately rather than under one cads/cts/ prefix, which also
    /// holds every other CT_* table the extract produces.</summary>
    [Fact]
    public void CtsLocationIdentifiers_ShouldDiscoverBothLanesAndNoSiblingTables()
    {
        // Arrange
        var definition = StandardDataSetDefinitionsBuilder.Build().CtsLocationIdentifiers!;

        // Act
        var prefixes = DataSetFileNaming.ListingPrefixes(definition);

        // Assert
        prefixes.Should().Equal("cads/cts/bulk/", "cads/cts/daily/");

        DataSetFileNaming.Matches(definition,
            "cads/cts/bulk/CT_LOCATION_IDENTIFIERS_2026-08-22-072826.xsvn.csv")
            .Should().BeTrue();
        DataSetFileNaming.Matches(definition,
            "cads/cts/daily/CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.xsvn.csv")
            .Should().BeTrue();
        DataSetFileNaming.Matches(definition,
            "cads/cts/daily/CTSM_CADS_PROD_DELTA_00002_001_CT_ADDRESSES_2026-08-23-063010.xsvn.csv")
            .Should().BeFalse();
    }

    /// <summary>Only the bulk lane feeds the baseline hash. A normalised key no longer carries the folder
    /// it arrived in, so the baseline is told apart by the name: the sample's bulk file is named for the
    /// table alone, where a delta carries the run that produced it ahead of it.</summary>
    [Fact]
    public void CtsLocationIdentifiers_ShouldTellTheBaselineLaneApartInBothSourceAndNormalisedKeys()
    {
        // Arrange
        var definition = StandardDataSetDefinitionsBuilder.Build().CtsLocationIdentifiers!;

        // Act & Assert
        DataSetFileNaming.MatchesBaseline(definition,
            "cads/cts/bulk/CT_LOCATION_IDENTIFIERS_2026-08-22-072826.xsvn.csv")
            .Should().BeTrue();
        DataSetFileNaming.MatchesBaseline(definition,
            "cts_location_identifiers/CT_LOCATION_IDENTIFIERS_2026-08-22-072826.xsvn.parquet")
            .Should().BeTrue();
        DataSetFileNaming.MatchesBaseline(definition,
            "cts_location_identifiers/CT_LOCATION_IDENTIFIERS_002_2026-08-22-072826.xsvn.parquet")
            .Should().BeTrue();
        DataSetFileNaming.MatchesBaseline(definition,
            "cts_location_identifiers/CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072826.xsvn.parquet")
            .Should().BeTrue();
        DataSetFileNaming.MatchesBaseline(definition,
            "cts_location_identifiers/CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.xsvn.parquet")
            .Should().BeFalse();
    }
}
