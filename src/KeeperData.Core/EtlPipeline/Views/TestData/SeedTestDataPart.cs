using System.Globalization;
using System.Text;

namespace KeeperData.Core.EtlPipeline.Views.TestData;

/// <summary>The personas as the transformation's last part.
///
/// It runs after the read model is built and on the same connection, so it derives ids from the
/// macros the real rows were keyed by rather than reimplementing them. That is what makes a persona
/// naming a CPH the extracts also carry land on that holding's row instead of beside it.
///
/// A seeded holding is claimed outright: its herds, profiles and roles are cleared before the
/// persona's are written, so a test holding carries what the persona says rather than that plus
/// whatever the day's extract attached to the same CPH.</summary>
public static class SeedTestDataPart
{
    public const string PartName = "krds-test-data";

    /// <summary>Tables the part writes to. All are produced by the read model first, so the writer
    /// counts them a second time once the seed has run.</summary>
    private static readonly string[] Tables = ["Party", "Holding", "Herd", "HoldingAnimalProfile", "PartyRole"];

    public static SqliteViewPart Create(IReadOnlyList<TestPersona> personas)
        => new(PartName, Sql(personas), [], Tables, Required: false);

    internal static string Sql(IReadOnlyList<TestPersona> personas)
    {
        var builder = new StringBuilder();

        foreach (var persona in personas)
        {
            AppendPersona(builder, persona);
        }

        return builder.ToString();
    }

    private static void AppendPersona(StringBuilder builder, TestPersona persona)
    {
        var partyId = $"source_guid('party', {Literal(persona.Party.SourcePartyId)})";

        builder.AppendLine($"-- {persona.Name}");

        // Every delete precedes every insert: clearing a holding's roles after the party's were
        // written would take the persona's own back out again.
        builder.AppendLine($"DELETE FROM target.PartyRole WHERE PartyId = {partyId};");
        builder.AppendLine($"DELETE FROM target.Party WHERE Id = {partyId};");

        foreach (var holding in persona.Holdings)
        {
            var holdingId = HoldingId(holding.Cph);

            builder.AppendLine($"DELETE FROM target.PartyRole WHERE HoldingId = {holdingId};");
            builder.AppendLine($"DELETE FROM target.HoldingAnimalProfile WHERE HoldingId = {holdingId};");
            builder.AppendLine($"DELETE FROM target.Herd WHERE HoldingId = {holdingId};");
            builder.AppendLine($"DELETE FROM target.Holding WHERE Id = {holdingId};");
        }

        AppendParty(builder, persona.Party, partyId);

        foreach (var holding in persona.Holdings)
        {
            AppendHolding(builder, holding);

            foreach (var herd in holding.Herds)
            {
                AppendHerd(builder, holding, herd);
            }

            foreach (var profile in holding.AnimalProfiles)
            {
                AppendAnimalProfile(builder, holding, profile);
            }

            foreach (var role in holding.Roles)
            {
                AppendRole(builder, persona.Party, holding, role, partyId);
            }
        }

        builder.AppendLine();
    }

    private static void AppendParty(StringBuilder builder, TestParty party, string partyId)
        => AppendInsert(builder, "Party",
            ("Id", partyId),
            ("SourcePartyId", Literal(party.SourcePartyId)),
            ("PersonTitle", Literal(party.PersonTitle)),
            ("GivenName", Literal(party.GivenName)),
            ("GivenName2", Literal(party.GivenName2)),
            ("Initials", Literal(party.Initials)),
            ("FamilyName", Literal(party.FamilyName)),
            ("OrganisationName", Literal(party.OrganisationName)),
            ("Telephone", Literal(party.Telephone)),
            ("Mobile", Literal(party.Mobile)),
            ("Email", Literal(party.Email)),
            ("AddressLine1", Literal(party.AddressLine1)),
            ("AddressStreet", Literal(party.AddressStreet)),
            ("AddressTown", Literal(party.AddressTown)),
            ("AddressLocality", Literal(party.AddressLocality)),
            ("AddressNation", Literal(party.AddressNation)),
            ("AddressPostcode", Literal(party.AddressPostcode)),
            ("AddressCountryCode", Literal(party.AddressCountryCode)),
            ("Roles", Literal(party.Roles)));

    private static void AppendHolding(StringBuilder builder, TestHolding holding)
        => AppendInsert(builder, "Holding",
            ("Id", HoldingId(holding.Cph)),
            ("Cph", Literal(holding.Cph)),
            ("FeatureName", Literal(holding.FeatureName)),
            ("CphType", Literal(holding.CphType)),
            ("StartDate", EpochSeconds(holding.StartDate)),
            ("EndDate", EpochSeconds(holding.EndDate)),
            ("AddressPk", Literal(holding.AddressPk)),
            ("SaonStartNumber", Literal(holding.SaonStartNumber)),
            ("SaonStartNumberSuffix", Literal(holding.SaonStartNumberSuffix)),
            ("SaonEndNumber", Literal(holding.SaonEndNumber)),
            ("SaonEndNumberSuffix", Literal(holding.SaonEndNumberSuffix)),
            ("SaonDescription", Literal(holding.SaonDescription)),
            ("PaonStartNumber", Literal(holding.PaonStartNumber)),
            ("PaonStartNumberSuffix", Literal(holding.PaonStartNumberSuffix)),
            ("PaonEndNumber", Literal(holding.PaonEndNumber)),
            ("PaonEndNumberSuffix", Literal(holding.PaonEndNumberSuffix)),
            ("PaonDescription", Literal(holding.PaonDescription)),
            ("Street", Literal(holding.Street)),
            ("Town", Literal(holding.Town)),
            ("Locality", Literal(holding.Locality)),
            ("UkInternalCode", Literal(holding.UkInternalCode)),
            ("Postcode", Literal(holding.Postcode)),
            ("CountryCode", Literal(holding.CountryCode)),
            ("Udprn", Number(holding.Udprn)),
            ("Easting", Number(holding.Easting)),
            ("Northing", Number(holding.Northing)),
            ("OsMapReference", Literal(holding.OsMapReference)));

    private static void AppendHerd(StringBuilder builder, TestHolding holding, TestHerd herd)
        => AppendInsert(builder, "Herd",
            ("Id", HerdId(herd)),
            ("HoldingId", HoldingId(holding.Cph)),
            ("Herdmark", Literal(herd.Herdmark)),
            ("Cphh", Literal(herd.Cphh)),
            ("AnimalSpeciesCode", Literal(herd.AnimalSpeciesCode)),
            ("AnimalPurposeCode", Literal(herd.AnimalPurposeCode)),
            ("DiseaseType", Literal(herd.DiseaseType)),
            ("Intervals", Literal(herd.Intervals)),
            ("IntervalUnitOfTime", Literal(herd.IntervalUnitOfTime)),
            ("MovementRestrictionReasonCode", Literal(herd.MovementRestrictionReasonCode)),
            ("AnimalGroupFromDate", EpochSeconds(herd.AnimalGroupFromDate)),
            ("AnimalGroupToDate", EpochSeconds(herd.AnimalGroupToDate)));

    private static void AppendAnimalProfile(StringBuilder builder, TestHolding holding, TestAnimalProfile profile)
    {
        var key = string.Join('|',
            holding.Cph,
            profile.AnimalSpeciesCode,
            profile.AnimalProductionUsageCode ?? string.Empty,
            profile.DiseaseType ?? string.Empty,
            profile.Interval ?? string.Empty,
            profile.IntervalUnitOfTime ?? string.Empty);

        AppendInsert(builder, "HoldingAnimalProfile",
            ("Id", $"source_guid('holding-animal-profile', {Literal(key)})"),
            ("HoldingId", HoldingId(holding.Cph)),
            ("AnimalSpeciesCode", Literal(profile.AnimalSpeciesCode)),
            ("AnimalProductionUsageCode", Literal(profile.AnimalProductionUsageCode)),
            ("DiseaseType", Literal(profile.DiseaseType)),
            ("Interval", Literal(profile.Interval)),
            ("IntervalUnitOfTime", Literal(profile.IntervalUnitOfTime)));
    }

    private static void AppendRole(
        StringBuilder builder,
        TestParty party,
        TestHolding holding,
        TestPartyRole role,
        string partyId)
    {
        var herdId = role.Cphh is null ? "NULL" : HerdId(Herd(holding, role.Cphh));

        // The read model keys a role on the herd's id rather than its CPHH, so the key is built in
        // SQL from the same expression the id itself came from.
        var key = $"{Literal($"{party.SourcePartyId}|{holding.Cph}|")} || COALESCE({herdId}, '') || {Literal($"|{role.Role}")}";

        AppendInsert(builder, "PartyRole",
            ("Id", $"source_guid('party-role', {key})"),
            ("PartyId", partyId),
            ("HoldingId", HoldingId(holding.Cph)),
            ("HerdId", herdId),
            ("Role", Literal(role.Role)));
    }

    private static TestHerd Herd(TestHolding holding, string cphh)
        => holding.Herds.FirstOrDefault(herd => herd.Cphh == cphh)
            ?? throw new InvalidOperationException(
                $"Holding '{holding.Cph}' has a role against herd '{cphh}', which it does not declare.");

    private static string HoldingId(string cph) => $"holding_guid({Literal(cph)})";

    private static string HerdId(TestHerd herd) => $"source_guid('herd', {Literal($"{herd.Herdmark}|{herd.Cphh}")})";

    private static void AppendInsert(StringBuilder builder, string table, params (string Column, string Value)[] columns)
    {
        builder.AppendLine($"INSERT INTO target.{table} ({string.Join(", ", columns.Select(column => column.Column))})");
        builder.AppendLine($"VALUES ({string.Join(", ", columns.Select(column => column.Value))});");
    }

    private static string Literal(string? value)
        => value is null ? "NULL" : $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";

    /// <summary>Cast because the target columns are 64-bit and an unqualified literal is not.</summary>
    private static string Number(long? value)
        => value is null ? "NULL" : $"{value.Value.ToString(CultureInfo.InvariantCulture)}::BIGINT";

    /// <summary>Date-only and read as UTC, as the transformation reads the source's own dates.</summary>
    private static string EpochSeconds(DateOnly? value)
        => value is null
            ? "NULL"
            : Number(new DateTimeOffset(value.Value.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero).ToUnixTimeSeconds());
}
