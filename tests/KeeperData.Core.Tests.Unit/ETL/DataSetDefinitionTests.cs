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
    public void DataSetDefinitions_ShouldRegisterTheCtsTablesAlongsideTheLitprdDatasets()
    {
        // Act
        var definitions = StandardDataSetDefinitionsBuilder.Build();

        // Assert
        definitions.All.Should().HaveCount(18, "the twelve litprd datasets and the six CTS tables");
        definitions.CtsDataSets.Select(dataset => dataset.Name).Should().Equal(
            "cts_location_identifiers",
            "cts_locations",
            "cts_location_party_rels",
            "cts_parties",
            "cts_addresses",
            "cts_counties");
        definitions.All.Should().Contain(definitions.CtsDataSets);
        definitions.All.Should().Contain(definitions.CtsLocationIdentifiers!);
    }

    /// <summary>Every CTS table is described the same way, so what is worth asserting per dataset is the
    /// three things that differ: the source table its files are named for, its key, and the audit
    /// columns the deltas carry - all of which are dropped from the snapshot.</summary>
    [Theory]
    [InlineData("cts_locations", "CT_LOCATIONS", "LOC")]
    [InlineData("cts_location_party_rels", "CT_LOCATION_PARTY_RELS", "LPR")]
    [InlineData("cts_parties", "CT_PARTIES", "PAR")]
    [InlineData("cts_addresses", "CT_ADDRESSES", "ADR")]
    [InlineData("cts_counties", "CT_COUNTIES", "CTY")]
    [InlineData("cts_location_identifiers", "CT_LOCATION_IDENTIFIERS", "LID")]
    public void CtsDataSets_ShouldDescribeEachTableByItsSourceNameKeyAndAuditColumns(
        string name,
        string table,
        string columnPrefix)
    {
        // Arrange
        var definition = StandardDataSetDefinitionsBuilder.Build().CtsDataSets
            .Single(dataset => dataset.Name == name);

        // Assert
        definition.PrimaryKeyHeaderNames.Should().Equal($"{columnPrefix}_ID");
        definition.ChangeTypeHeaderName.Should().Be($"{columnPrefix}_AUD_TYPE");
        definition.Audit.Should().Be(new AuditColumns($"{columnPrefix}_AUD_ID", $"{columnPrefix}_AUD_DATETIME"));
        definition.ExcludedColumns.Should().Equal(
            $"{columnPrefix}_AUD_ID",
            $"{columnPrefix}_AUD_TYPE",
            $"{columnPrefix}_AUD_DATETIME",
            "RECORD_TYPE",
            "RECORD_COUNT");
        definition.Accumulators.Should().BeEmpty();
        definition.DateTimePattern.Should().Be("yyyy-MM-dd-HHmmss");
        definition.Format.Should().Be(FileFormat.Hcdt);
        definition.IngestionMode.Should().Be(DataSetIngestionMode.Delta);
        definition.PasswordDerivation.Should().Be(PasswordDerivationPolicy.CtsDerived);
        definition.SourceKeyPattern.Should().Be($"cads/cts/{{bulk,daily}}/*_{table}_*");
        definition.BaselineKeyPattern.Should().Be($"cads/cts/bulk/*_BULK_*_{table}_*");
    }

    /// <summary>The same table is published as PREP by preprod and PROD by production, so the
    /// environment is the file's to state, not ours to require.</summary>
    [Theory]
    [InlineData("PREP")]
    [InlineData("PROD")]
    public void CtsDataSets_ShouldDiscoverATableInEveryEnvironmentItIsPublishedBy(string environment)
    {
        // Arrange
        var definition = StandardDataSetDefinitionsBuilder.Build().CtsDataSets
            .Single(dataset => dataset.Name == "cts_locations");

        // Act & Assert
        DataSetFileNaming.Matches(definition,
            $"cads/cts/bulk/CTSM_CADS_{environment}_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv")
            .Should().BeTrue();
        DataSetFileNaming.Matches(definition,
            $"cads/cts/daily/CTSM_CADS_{environment}_DELTA_00002_001_CT_LOCATIONS_2026-07-30-141209.csv")
            .Should().BeTrue();
    }

    /// <summary>Three of the six table names are prefixes of each other, so a pattern that did not
    /// anchor the table between underscores would quietly ingest a sibling's files as its own.</summary>
    [Fact]
    public void CtsDataSets_ShouldNotCaptureASiblingTableWhoseNameExtendsItsOwn()
    {
        // Arrange
        var datasets = StandardDataSetDefinitionsBuilder.Build().CtsDataSets;
        var keys = new[]
        {
            "cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv",
            "cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-07-28-094638.csv",
            "cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_LOCATION_PARTY_RELS_2026-07-28-094630.csv",
            "cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_PARTIES_2026-07-28-094628.csv"
        };

        // Act
        var matched = keys.Select(key =>
            datasets.Count(dataset => DataSetFileNaming.Matches(dataset, key)));

        // Assert
        matched.Should().AllSatisfy(count => count.Should().Be(1));
    }

    /// <summary>The lanes are listed separately rather than under one cads/cts/ prefix, which also holds
    /// the eighty-odd other CT_* tables the extract produces.</summary>
    [Fact]
    public void CtsDataSets_ShouldListOnlyTheTwoLanesItsFilesArriveIn()
    {
        // Arrange
        var datasets = StandardDataSetDefinitionsBuilder.Build().CtsDataSets;

        // Act & Assert
        datasets.Should().AllSatisfy(dataset =>
            DataSetFileNaming.ListingPrefixes(dataset).Should().Equal("cads/cts/bulk/", "cads/cts/daily/"));
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
        definition.Format.Should().Be(FileFormat.Hcdt);
        definition.PasswordDerivation.Should().Be(PasswordDerivationPolicy.CtsDerived);
        definition.SourceKeyPattern.Should().Be("cads/cts/{bulk,daily}/*_CT_LOCATION_IDENTIFIERS_*");
        definition.BaselineKeyPattern.Should().Be("cads/cts/bulk/*_BULK_*_CT_LOCATION_IDENTIFIERS_*");
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
            "cads/cts/bulk/CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072826.csv")
            .Should().BeTrue();
        DataSetFileNaming.Matches(definition,
            "cads/cts/daily/CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.csv")
            .Should().BeTrue();
        DataSetFileNaming.Matches(definition,
            "cads/cts/daily/CTSM_CADS_PROD_DELTA_00002_001_CT_ADDRESSES_2026-08-23-063010.csv")
            .Should().BeFalse();
    }

    /// <summary>Only the bulk lane feeds the baseline hash. A normalised key no longer carries the folder
    /// it arrived in, so the baseline is told apart by the name: both lanes name the run that produced
    /// the file, and a baseline run is a _BULK_ one.</summary>
    [Fact]
    public void CtsLocationIdentifiers_ShouldTellTheBaselineLaneApartInBothSourceAndNormalisedKeys()
    {
        // Arrange
        var definition = StandardDataSetDefinitionsBuilder.Build().CtsLocationIdentifiers!;

        // Act & Assert
        DataSetFileNaming.MatchesBaseline(definition,
            "cads/cts/bulk/CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072826.csv")
            .Should().BeTrue();
        DataSetFileNaming.MatchesBaseline(definition,
            "cts_location_identifiers/CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072826.parquet")
            .Should().BeTrue();
        DataSetFileNaming.MatchesBaseline(definition,
            "cts_location_identifiers/CTSM_CADS_PROD_BULK_00001_002_CT_LOCATION_IDENTIFIERS_2026-08-22-072826.parquet")
            .Should().BeTrue();
        DataSetFileNaming.MatchesBaseline(definition,
            "cts_location_identifiers/CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.parquet")
            .Should().BeFalse();
    }
}
