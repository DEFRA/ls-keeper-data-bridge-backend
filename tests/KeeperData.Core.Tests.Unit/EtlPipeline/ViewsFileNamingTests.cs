using FluentAssertions;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.EtlPipeline.Views;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

public class ViewsFileNamingTests
{
    [Fact]
    public void Names_the_export_after_the_source_timestamp_not_the_run_time()
    {
        var key = ViewsFileNaming.DatabaseKey(new DateTimeOffset(2026, 8, 21, 7, 0, 3, TimeSpan.Zero));

        key.Should().Be("krds-db_20260821070003.sqlite");
    }

    [Fact]
    public void Converts_the_timestamp_to_utc_so_one_instant_has_one_key()
    {
        var utc = ViewsFileNaming.DatabaseKey(new DateTimeOffset(2026, 8, 21, 7, 0, 3, TimeSpan.Zero));
        var offset = ViewsFileNaming.DatabaseKey(new DateTimeOffset(2026, 8, 21, 9, 0, 3, TimeSpan.FromHours(2)));

        offset.Should().Be(utc);
    }

    [Theory]
    [InlineData("krds-db_20260821070003.sqlite", true)]
    [InlineData("krds-db_20260821070003.SQLITE", true)]
    [InlineData("cphs_20260102T120000Z.sqlite", false)]
    [InlineData("archive/krds-db_20260821070003.sqlite", false)]
    [InlineData("other-krds-db_20260821070003.sqlite", false)]
    [InlineData("krds-db_latest.sqlite", false)]
    [InlineData("krds-db_20261301070003.sqlite", false)]
    [InlineData("krds-db_20260821070003.duckdb", false)]
    [InlineData("readme.txt", false)]
    public void Recognises_only_its_own_exports(string key, bool expected)
        => ViewsFileNaming.IsDatabaseKey(key).Should().Be(expected);
}

public class SqliteViewDefinitionTests
{
    [Fact]
    public void Carries_the_transformation_in_the_assembly()
        => SqliteViewDefinition.Sql.Should().Contain("INSERT INTO target.Party");

    [Fact]
    public void Leaves_the_connection_statements_to_the_writer()
    {
        // The writer owns these because the paths are only known at run time; a script that carried
        // its own would need them substituted in. The comment header still shows them, so only the
        // executable statements are considered here.
        var statements = string.Join(
            '\n',
            SqliteViewDefinition.Sql
                .Split('\n')
                .Where(line => !line.TrimStart().StartsWith("--", StringComparison.Ordinal)));

        statements.Should().NotContain("ATTACH ");
        statements.Should().NotContain("DETACH ");
        statements.Should().NotContain("INSTALL ");
        statements.Should().NotContain("CHECKPOINT ");
    }

    [Fact]
    public void Fingerprints_the_transformation_so_a_changed_script_can_be_detected()
        => SqliteViewDefinition.Version.Should().MatchRegex("^v[0-9]+-[0-9a-f]{16}$");

    [Fact]
    public void Declares_every_table_the_transformation_creates()
    {
        foreach (var table in SqliteViewDefinition.TableNames)
        {
            SqliteViewDefinition.Sql.Should().Contain($"CREATE TABLE target.{table} (");
        }
    }
}
