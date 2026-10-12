using System.Globalization;
using System.Text;

namespace KeeperData.Core.EtlPipeline.Views.TestData;

/// <summary>The personas as the transformation's last part.
///
/// It runs after the read model is built and on the same connection, so it derives ids from the
/// macros the real rows were keyed by rather than reimplementing them. That is what lets a persona
/// address a holding the extracts also carry.
///
/// Nothing here may stop the export. Every statement stands alone and is guarded by what it needs:
/// a row is written only where it is absent and the rows it points at are present, so a holding
/// that never arrived leaves the persona inert rather than dangling. Which statements are written
/// at all is decided here, in C#, so a malformed persona produces less SQL rather than failing
/// SQL.</summary>
public static class SeedTestDataPart
{
    public const string PartName = "krds-test-data";

    public static SqliteViewPart Create(IReadOnlyList<TestPersona> personas)
        => new(PartName, Sql(personas), [], TablesFor(personas), Required: false, ToleratesFailure: true);

    internal static string Sql(IReadOnlyList<TestPersona> personas)
    {
        var builder = new StringBuilder();

        // Every claim is cleared before anything is written, so a persona attached to a holding
        // another persona owns cannot have its rows deleted by that claim afterwards.
        foreach (var holding in personas.SelectMany(persona => persona.Holdings).Where(holding => holding.Claimed))
        {
            AppendClaim(builder, holding);
        }

        foreach (var persona in personas)
        {
            AppendPersona(builder, persona);
        }

        return builder.ToString();
    }

    /// <summary>What these personas write to, so the export counts those tables again and claims
    /// credit for no others.</summary>
    private static IReadOnlyList<string> TablesFor(IReadOnlyList<TestPersona> personas)
    {
        var holdings = personas.SelectMany(persona => persona.Holdings).ToArray();
        var claimed = holdings.Where(holding => holding.Claimed).ToArray();

        List<string> tables = [];

        if (personas.Count > 0)
        {
            tables.Add("Party");
        }

        if (claimed.Length > 0)
        {
            tables.Add("Holding");
        }

        if (claimed.Any(holding => holding.Herds.Count > 0))
        {
            tables.Add("Herd");
        }

        if (claimed.Any(holding => holding.AnimalProfiles.Count > 0))
        {
            tables.Add("HoldingAnimalProfile");
        }

        if (holdings.Any(holding => holding.Roles.Count > 0))
        {
            tables.Add("PartyRole");
        }

        return tables;
    }

    private static void AppendClaim(StringBuilder builder, TestHolding holding)
    {
        var holdingId = HoldingId(holding.Cph);

        builder.AppendLine($"-- claim {holding.Cph}");
        builder.AppendLine($"DELETE FROM target.PartyRole WHERE HoldingId = {holdingId};");
        builder.AppendLine($"DELETE FROM target.HoldingAnimalProfile WHERE HoldingId = {holdingId};");
        builder.AppendLine($"DELETE FROM target.Herd WHERE HoldingId = {holdingId};");
        builder.AppendLine($"DELETE FROM target.Holding WHERE Id = {holdingId};");
        builder.AppendLine();
    }

    private static void AppendPersona(StringBuilder builder, TestPersona persona)
    {
        builder.AppendLine($"-- {persona.Name}");

        AppendParty(builder, persona.Party);

        foreach (var holding in persona.Holdings)
        {
            // An unclaimed holding belongs to the extract. The persona attaches roles to it and
            // leaves its own row, herds and profiles exactly as they arrived.
            if (holding.Claimed)
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
            }

            foreach (var role in holding.Roles)
            {
                AppendRole(builder, persona.Party, holding, role);
            }
        }

        builder.AppendLine();
    }

    private static void AppendParty(StringBuilder builder, TestParty party)
        => AppendInsert(builder, "Party",
            [$"NOT EXISTS (SELECT 1 FROM target.Party WHERE Id = {PartyId(party)} OR SourcePartyId = {Literal(party.SourcePartyId)})"],
            ("Id", PartyId(party)),
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
            [$"NOT EXISTS (SELECT 1 FROM target.Holding WHERE Id = {HoldingId(holding.Cph)} OR Cph = {Literal(holding.Cph)})"],
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
            [
                $"EXISTS (SELECT 1 FROM target.Holding WHERE Id = {HoldingId(holding.Cph)})",
                $"NOT EXISTS (SELECT 1 FROM target.Herd WHERE Id = {HerdId(herd)} OR (Herdmark = {Literal(herd.Herdmark)} AND Cphh = {Literal(herd.Cphh)}))"
            ],
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

        var id = $"source_guid('holding-animal-profile', {Literal(key)})";

        AppendInsert(builder, "HoldingAnimalProfile",
            [
                $"EXISTS (SELECT 1 FROM target.Holding WHERE Id = {HoldingId(holding.Cph)})",
                $"NOT EXISTS (SELECT 1 FROM target.HoldingAnimalProfile WHERE Id = {id})"
            ],
            ("Id", id),
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
        TestPartyRole role)
    {
        var herdId = "NULL";

        if (role.Cphh is not null)
        {
            var herd = holding.Herds.FirstOrDefault(candidate => candidate.Cphh == role.Cphh);

            // A role naming a herd its holding does not declare is a mistake in the personas, which
            // their own tests assert against. Here it simply writes nothing.
            if (herd is null)
            {
                return;
            }

            herdId = HerdId(herd);
        }

        // The read model keys a role on the herd's id rather than its CPHH, so the key is built in
        // SQL from the same expression the id itself came from.
        var key = $"{Literal($"{party.SourcePartyId}|{holding.Cph}|")} || COALESCE({herdId}, '') || {Literal($"|{role.Role}")}";
        var id = $"source_guid('party-role', {key})";

        List<string> guards =
        [
            $"EXISTS (SELECT 1 FROM target.Party WHERE Id = {PartyId(party)})",
            $"EXISTS (SELECT 1 FROM target.Holding WHERE Id = {HoldingId(holding.Cph)})"
        ];

        if (role.Cphh is not null)
        {
            guards.Add($"EXISTS (SELECT 1 FROM target.Herd WHERE Id = {herdId})");
        }

        guards.Add($"NOT EXISTS (SELECT 1 FROM target.PartyRole WHERE Id = {id})");

        AppendInsert(builder, "PartyRole", guards,
            ("Id", id),
            ("PartyId", PartyId(party)),
            ("HoldingId", HoldingId(holding.Cph)),
            ("HerdId", herdId),
            ("Role", Literal(role.Role)));
    }

    private static string PartyId(TestParty party) => $"source_guid('party', {Literal(party.SourcePartyId)})";

    private static string HoldingId(string cph) => $"holding_guid({Literal(cph)})";

    private static string HerdId(TestHerd herd) => $"source_guid('herd', {Literal($"{herd.Herdmark}|{herd.Cphh}")})";

    /// <summary>SELECT rather than VALUES so the row can be made conditional on what the database
    /// already holds.</summary>
    private static void AppendInsert(
        StringBuilder builder,
        string table,
        IReadOnlyList<string> guards,
        params (string Column, string Value)[] columns)
    {
        builder.AppendLine($"INSERT INTO target.{table} ({string.Join(", ", columns.Select(column => column.Column))})");
        builder.AppendLine($"SELECT {string.Join(", ", columns.Select(column => column.Value))}");
        builder.AppendLine($"WHERE {string.Join($"{Environment.NewLine}  AND ", guards)};");
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
