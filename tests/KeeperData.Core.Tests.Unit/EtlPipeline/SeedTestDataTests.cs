using FluentAssertions;
using KeeperData.Core.EtlPipeline.Views.TestData;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>The personas are data, so what is worth testing is that they can be turned into rows the
/// read model will accept, and that two of them never contend for the same row.</summary>
public class SeedTestDataTests
{
    private static IEnumerable<TestHolding> Holdings
        => SeedTestData.Personas.SelectMany(persona => persona.Holdings);

    [Fact]
    public void Finds_the_single_holding_keeper_by_the_email_and_role_a_caller_queries_on()
    {
        var persona = SeedTestData.Personas.Single(p => p.Party.Email == "defralivestock+single-holding@gmail.com");

        persona.Holdings
            .Where(holding => holding.Roles.Any(role => role.Role == "holder"))
            .Select(holding => holding.Cph)
            .Should().Equal("03/202/0021");
    }

    [Fact]
    public void Names_each_persona_once()
        => SeedTestData.Personas.Select(persona => persona.Name).Should().OnlyHaveUniqueItems();

    [Fact]
    public void Gives_each_persona_its_own_party()
    {
        SeedTestData.Personas.Select(persona => persona.Party.SourcePartyId).Should().OnlyHaveUniqueItems();
        SeedTestData.Personas.Select(persona => persona.Party.Email).Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void Keeps_the_source_party_ids_out_of_the_namespace_the_extracts_use()
    {
        foreach (var persona in SeedTestData.Personas)
        {
            persona.Party.SourcePartyId.Should().NotMatchRegex(
                "^[Cc][0-9]+$",
                "a real SAM party id is a C followed by digits, and seeding over a real party would hide them");
        }
    }

    [Fact]
    public void Stores_the_email_as_the_read_model_folds_it()
    {
        foreach (var persona in SeedTestData.Personas)
        {
            persona.Party.Email.Should().Be(
                persona.Party.Email.Trim().ToLowerInvariant(),
                "Email is indexed case-sensitively, so a mixed-case seed could not be looked up");
        }
    }

    [Fact]
    public void Claims_each_holding_for_one_persona_only()
        => Holdings.Select(holding => holding.Cph).Should().OnlyHaveUniqueItems();

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
