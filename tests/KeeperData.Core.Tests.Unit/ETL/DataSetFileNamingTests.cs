using FluentAssertions;
using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.Tests.Unit.ETL;

public class DataSetFileNamingTests
{
    private static readonly DataSetDefinition DateOnlyPatternDataSet =
        new("cts_keeper", "LITP_CTSKEEPER_{0}", ["KEY"], "change_type", []);

    private static readonly DataSetDefinition TimeBearingPatternDataSet =
        new("cts_agent", "LITP_CTSAGENT_{0}", ["KEY"], "change_type", [], DatePattern: "yyyyMMddHHmmss");

    [Fact]
    public void DatedKeyPrefix_FormatsTheDateIntoTheDataSetsPrefix()
    {
        var prefix = DataSetFileNaming.DatedKeyPrefix(DateOnlyPatternDataSet, new DateOnly(2024, 10, 15));

        prefix.Should().Be("LITP_CTSKEEPER_20241015");
    }

    [Fact]
    public void DatedKeyPrefix_UsesMiddayWhenThePatternCarriesATimeComponent()
    {
        var prefix = DataSetFileNaming.DatedKeyPrefix(TimeBearingPatternDataSet, new DateOnly(2024, 10, 15));

        prefix.Should().Be("LITP_CTSAGENT_20241015120000");
    }

    [Fact]
    public void ExtractTimestamp_ReadsTheTrailingTimestampAsUtc()
    {
        var timestamp = DataSetFileNaming.ExtractTimestamp(DateOnlyPatternDataSet, "LITP_CTSKEEPER_20241015133000.csv");

        timestamp.Should().Be(new DateTimeOffset(2024, 10, 15, 13, 30, 0, TimeSpan.Zero));
        timestamp.Offset.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void ExtractTimestamp_IgnoresEverythingAfterTheFirstDot()
    {
        var timestamp = DataSetFileNaming.ExtractTimestamp(DateOnlyPatternDataSet, "LITP_CTSKEEPER_20241015133000.csv.gpg");

        timestamp.Should().Be(new DateTimeOffset(2024, 10, 15, 13, 30, 0, TimeSpan.Zero));
    }

    [Theory]
    [InlineData("LITP_CTSKEEPER_notatimestamp.csv")]
    [InlineData("LITP_CTSKEEPER_2024.csv")]
    public void ExtractTimestamp_ThrowsWhenTheKeyCarriesNoParsableTimestamp(string key)
    {
        var extract = () => DataSetFileNaming.ExtractTimestamp(DateOnlyPatternDataSet, key);

        extract.Should().Throw<InvalidOperationException>().WithMessage("*Cannot extract timestamp*");
    }

    [Fact]
    public void ExtractTimestamp_ThrowsWhenTheKeyIsEmpty()
    {
        var extract = () => DataSetFileNaming.ExtractTimestamp(DateOnlyPatternDataSet, string.Empty);

        extract.Should().Throw<ArgumentException>();
    }
}
