using FluentAssertions;
using KeeperData.Core.EtlPipeline.Views.TestData;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>The personas are data, so what is worth testing is that they can be turned into rows the
/// read model will accept, that two of them never contend for the same row, and that each policy
/// only does what its name promises.
///
/// The generator is written never to fail, which means a malformed persona quietly produces less SQL
/// instead of failing SQL. These are what make that loud.</summary>
public class SeedTestDataTests
{
    public static TheoryData<TestDataPolicy> Policies => [TestDataPolicy.Additive, TestDataPolicy.Full];

    private static IEnumerable<TestPersona> AllPersonas
        => new[] { TestDataPolicy.Additive, TestDataPolicy.Full }.SelectMany(SeedTestData.For);

    private static IEnumerable<TestHolding> Holdings
        => AllPersonas.SelectMany(persona => persona.Holdings);

    private static IEnumerable<TestHolding> HoldingsOf(TestDataPolicy policy)
        => SeedTestData.For(policy).SelectMany(persona => persona.Holdings);

    [Fact]
    public void Seeds_nothing_at_all_without_a_policy()
        => SeedTestData.For(TestDataPolicy.None).Should().BeEmpty();

    /// <summary>The acceptance criterion for production: one keeper, holder of two existing CPHs.</summary>
    [Fact]
    public void Attaches_the_production_keeper_to_both_holdings_as_holder()
    {
        var persona = SeedTestData.For(TestDataPolicy.Additive).Single();

        persona.Party.Email.Should().Be("defralivestock@gmail.com");
        persona.Party.SourcePartyId.Should().Be("CT35TP4RTY");

        persona.Holdings.Select(holding => holding.Cph)
            .Should().BeEquivalentTo(["08/205/8000", "03/202/0021"]);

        persona.Holdings.SelectMany(holding => holding.Roles).Select(role => role.Role)
            .Should().AllBe("holder");
    }

    /// <summary>The whole point of the additive policy. A claimed holding would delete the real
    /// herds, animal profiles and party roles of whatever shares its CPH.</summary>
    [Fact]
    public void Claims_nothing_under_the_additive_policy()
        => HoldingsOf(TestDataPolicy.Additive).Should().OnlyContain(holding => !holding.Claimed);

    /// <summary>An unclaimed holding belongs to the extract, so anything a persona says about it
    /// beyond its roles is written nowhere and would mislead the next reader.</summary>
    [Fact]
    public void Describes_nothing_about_a_holding_it_does_not_claim()
    {
        foreach (var holding in Holdings.Where(holding => !holding.Claimed))
        {
            holding.Should().BeEquivalentTo(
                new TestHolding { Cph = holding.Cph, Roles = holding.Roles },
                "{0} is not claimed, so only its CPH and roles reach the read model", holding.Cph);
        }
    }

    [Fact]
    public void Finds_the_single_holding_keeper_by_the_email_and_role_a_caller_queries_on()
    {
        var persona = SeedTestData.For(TestDataPolicy.Full)
            .Single(p => p.Party.Email == "defralivestock+single-holding@gmail.com");

        persona.Holdings
            .Where(holding => holding.Roles.Any(role => role.Role == "holder"))
            .Select(holding => holding.Cph)
            .Should().Equal("03/202/0021");
    }

    [Theory]
    [MemberData(nameof(Policies))]
    public void Names_each_persona_once(TestDataPolicy policy)
        => SeedTestData.For(policy).Select(persona => persona.Name).Should().OnlyHaveUniqueItems();

    [Fact]
    public void Gives_each_persona_its_own_party()
    {
        AllPersonas.Select(persona => persona.Party.SourcePartyId).Should().OnlyHaveUniqueItems();
        AllPersonas.Select(persona => persona.Party.Email).Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void Keeps_the_source_party_ids_out_of_the_namespace_the_extracts_use()
    {
        foreach (var persona in AllPersonas)
        {
            persona.Party.SourcePartyId.Should().NotMatchRegex(
                "^[Cc][0-9]+$",
                "a real SAM party id is a C followed by digits, and seeding over a real party would hide them");
        }
    }

    [Fact]
    public void Stores_the_email_as_the_read_model_folds_it()
    {
        foreach (var persona in AllPersonas)
        {
            persona.Party.Email.Should().Be(
                persona.Party.Email.Trim().ToLowerInvariant(),
                "Email is indexed case-sensitively, so a mixed-case seed could not be looked up");
        }
    }

    [Theory]
    [MemberData(nameof(Policies))]
    public void Claims_each_holding_for_one_persona_only(TestDataPolicy policy)
        => HoldingsOf(policy).Select(holding => holding.Cph).Should().OnlyHaveUniqueItems();

    [Fact]
    public void Shapes_every_cph_as_the_read_model_expects()
    {
        foreach (var holding in Holdings)
        {
            holding.Cph.Should().MatchRegex("^[0-9]{2}/[0-9]{3}/[0-9]{4}$");
        }
    }

    [Fact]
    public void Shapes_every_cphh_as_its_own_holding_plus_a_group_suffix()
    {
        foreach (var holding in Holdings)
        {
            foreach (var herd in holding.Herds)
            {
                herd.Cphh.Should().MatchRegex("^[0-9]{2}/[0-9]{3}/[0-9]{4}/[0-9]{2}$");
                herd.Cphh.Should().StartWith(holding.Cph + "/");
            }
        }
    }

    [Fact]
    public void Identifies_each_herd_once()
        => Holdings.SelectMany(holding => holding.Herds)
            .Select(herd => (herd.Herdmark, herd.Cphh))
            .Should().OnlyHaveUniqueItems();

    [Fact]
    public void Declares_each_animal_profile_once_per_holding()
    {
        foreach (var holding in Holdings)
        {
            holding.AnimalProfiles
                .Select(profile => (profile.AnimalSpeciesCode, profile.AnimalProductionUsageCode,
                    profile.DiseaseType, profile.Interval, profile.IntervalUnitOfTime))
                .Should().OnlyHaveUniqueItems();
        }
    }

    [Fact]
    public void Uses_only_the_roles_the_read_model_allows()
    {
        foreach (var holding in Holdings)
        {
            holding.Roles.Select(role => role.Role).Should().BeSubsetOf(["owner", "holder", "keeper"]);
        }
    }

    [Fact]
    public void Declares_each_role_once_per_holding()
    {
        foreach (var holding in Holdings)
        {
            holding.Roles.Should().OnlyHaveUniqueItems();
        }
    }

    /// <summary>The generator skips a role naming a herd that is not there rather than failing, so
    /// without this a typo would simply lose the role.</summary>
    [Fact]
    public void Points_every_herd_level_role_at_a_herd_of_its_own_holding()
    {
        foreach (var holding in Holdings)
        {
            var herds = holding.Herds.Select(herd => herd.Cphh).ToArray();

            holding.Roles
                .Where(role => role.Cphh is not null)
                .Select(role => role.Cphh!)
                .Should().BeSubsetOf(herds);
        }
    }
}
