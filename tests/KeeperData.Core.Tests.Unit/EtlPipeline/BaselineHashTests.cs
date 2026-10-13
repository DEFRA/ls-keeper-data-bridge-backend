using FluentAssertions;
using KeeperData.Core.EtlPipeline.Storage;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>The reset trigger: a hash of the set of bulk file names. Bulk files are a set, so the hash
/// must not depend on the order they were discovered in, and must move the moment the set does.</summary>
public class BaselineHashTests
{
    private const string PartOne = "cts_location_identifiers/CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_20260822072824.parquet";
    private const string PartTwo = "cts_location_identifiers/CTSM_CADS_PROD_BULK_00001_002_CT_LOCATION_IDENTIFIERS_20260822072824.parquet";

    [Fact]
    public void Is_eight_hex_characters()
        => BaselineHash.Compute([PartOne]).Should().MatchRegex("^[0-9a-f]{8}$");

    [Fact]
    public void Does_not_depend_on_the_order_the_bulk_files_were_discovered_in()
        => BaselineHash.Compute([PartOne, PartTwo]).Should().Be(BaselineHash.Compute([PartTwo, PartOne]));

    [Fact]
    public void Changes_when_a_bulk_part_is_added()
        => BaselineHash.Compute([PartOne, PartTwo]).Should().NotBe(BaselineHash.Compute([PartOne]));

    [Fact]
    public void Changes_when_a_bulk_part_is_removed()
        => BaselineHash.Compute([PartOne]).Should().NotBe(BaselineHash.Compute([PartOne, PartTwo]));

    /// <summary>Otherwise one name holding a newline would hash as the two names either side of it, and
    /// the hash is what decides whether the dataset rebuilds.</summary>
    [Fact]
    public void Refuses_a_name_carrying_the_delimiter_rather_than_hashing_it_as_two()
    {
        var hashing = () => BaselineHash.Compute([$"{PartOne}\n{PartTwo}"]);

        hashing.Should().Throw<ArgumentException>().WithMessage("*carries a newline*");
    }

    /// <summary>Which is what keeps the mechanism inert for a dataset with no bulk lane.</summary>
    [Fact]
    public void Is_constant_for_the_empty_set()
        => BaselineHash.Compute([]).Should().Be("e3b0c442");
}
