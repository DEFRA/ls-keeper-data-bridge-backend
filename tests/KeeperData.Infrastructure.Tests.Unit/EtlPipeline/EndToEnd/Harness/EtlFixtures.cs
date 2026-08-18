using System.Text;
using KeeperData.Core.ETL.Impl;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd.Harness;

/// <summary>
/// The source files the end-to-end suite runs on: three datasets, three dated files each, written
/// out as literals.
///
/// Literals rather than the Bogus generator on purpose. Generated data with a fixed seed is
/// reproducible but opaque, and a failure diff of invented company names tells you nothing. These
/// fixtures are small enough to read, so the expected end state can be stated as a table and a
/// failure points straight at the row that moved.
///
/// The three datasets are chosen to cover the interesting axes in one run: composite keys of four,
/// three and one column, and both ingestion modes (sam_cph_holdings is Delta, the other two are
/// Snapshot).
///
/// Two pipeline behaviours shape what the fixtures can assert, and both are deliberate in the code:
///
/// 1. Delta mode ignores deletes. MergeState treats a D row as counted-and-skipped ("delete
///    processing is out of scope"), so the row it names stays in the snapshot at its previous value.
///    The sam_cph_holdings expectation therefore keeps the deleted row. Change this fixture when
///    delete processing is implemented, not before.
///
/// 2. Snapshot mode does not merge at all. SnapshotStage copies the dataset's LATEST normalised file
///    as-is, so each sam_herd and sam_party file has to be a complete statement of the world, not an
///    increment. The earlier files exist to prove the newest one wins, not to accumulate.
/// </summary>
public static class EtlFixtures
{
    public const string FirstTimestamp = "20251113121333";
    public const string SecondTimestamp = "20251114121333";
    public const string ThirdTimestamp = "20251115121333";

    /// <summary>Late enough on the last file's day that a 30-day lookback sees all three.</summary>
    public static readonly DateTimeOffset RunClock = new(2025, 11, 15, 18, 0, 0, TimeSpan.Zero);

    public static readonly DateTimeOffset LatestSourceTimestamp =
        new(2025, 11, 15, 12, 13, 33, TimeSpan.Zero);

    private static readonly DataSetDefinitions Standard = StandardDataSetDefinitionsBuilder.Build();

    public static DataSetDefinition CphHolding => Standard.SamCPHHolding;

    public static DataSetDefinition Herd => Standard.SamHerd;

    public static DataSetDefinition Party => Standard.SamParty;

    public static IReadOnlyList<DataSetDefinition> AllThree => [CphHolding, Herd, Party];

    /// <summary>Source file name for a dataset at one of the fixture timestamps.</summary>
    public static string FileName(DataSetDefinition definition, string timestamp)
        => $"{string.Format(definition.FilePrefixFormat, timestamp)}.csv";

    /// <summary>The three files for a dataset, oldest first, as (fileName, content) pairs.</summary>
    public static IReadOnlyList<(string FileName, string Content)> FilesFor(DataSetDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);

        if (definition.Name == CphHolding.Name)
        {
            return
            [
                (FileName(definition, FirstTimestamp), CphFirst),
                (FileName(definition, SecondTimestamp), CphSecond),
                (FileName(definition, ThirdTimestamp), CphThird)
            ];
        }

        if (definition.Name == Herd.Name)
        {
            return
            [
                (FileName(definition, FirstTimestamp), HerdFirst),
                (FileName(definition, SecondTimestamp), HerdSecond),
                (FileName(definition, ThirdTimestamp), HerdThird)
            ];
        }

        if (definition.Name == Party.Name)
        {
            return
            [
                (FileName(definition, FirstTimestamp), PartyFirst),
                (FileName(definition, SecondTimestamp), PartySecond),
                (FileName(definition, ThirdTimestamp), PartyThird)
            ];
        }

        throw new ArgumentOutOfRangeException(nameof(definition), definition.Name, "No fixture for this dataset.");
    }

    // sam_cph_holdings. Key: CPH + FEATURE_NAME + SECONDARY_CPH + ANIMAL_SPECIES_CODE.
    // Only CPH varies, so the four-part key is exercised without four varying columns to track.

    public const string CphHeader =
        "CPH|FEATURE_NAME|SECONDARY_CPH|ANIMAL_SPECIES_CODE|HOLDING_NAME|CHANGE_TYPE";

    private const string CphKeyTail = "MAIN|-|01";

    private static string CphFirst => Psv(CphHeader,
        $"01/001/0001|{CphKeyTail}|Old Farm|I",
        $"01/001/0002|{CphKeyTail}|Keep Farm|I");

    private static string CphSecond => Psv(CphHeader,
        $"01/001/0001|{CphKeyTail}|Updated Farm|U",
        $"01/001/0003|{CphKeyTail}|New Farm|I");

    private static string CphThird => Psv(CphHeader,
        $"01/001/0002|{CphKeyTail}|Keep Farm|D");

    /// <summary>Net effect of folding the three files in Delta mode: the update lands, the insert
    /// lands, and the D row for 01/001/0002 is ignored, so that row survives unchanged.</summary>
    public static (string Cph, string HoldingName)[] ExpectedCph =>
    [
        ("01/001/0001", "Updated Farm"),
        ("01/001/0002", "Keep Farm"),
        ("01/001/0003", "New Farm")
    ];

    /// <summary>The row the third file asks to delete. Still present, by current design.</summary>
    public const string CphDeletedKey = "01/001/0002";

    // sam_herd. Key: CPHH + HERDMARK + ANIMAL_PURPOSE_CODE.

    public const string HerdHeader = "CPHH|HERDMARK|ANIMAL_PURPOSE_CODE|HERD_NAME|CHANGE_TYPE";

    private static string HerdFirst => Psv(HerdHeader,
        "01/001/0001|AA1234|BR|Hill Herd|I",
        "01/001/0002|BB5678|DY|Vale Herd|I");

    // Each file is the whole world at that moment, because Snapshot mode copies the latest one.

    private static string HerdSecond => Psv(HerdHeader,
        "01/001/0001|AA1234|BR|Hill Herd|I",
        "01/001/0002|BB5678|DY|Vale Herd|I",
        "01/001/0003|DD3456|BR|Superseded Herd|I");

    private static string HerdThird => Psv(HerdHeader,
        "01/001/0001|AA1234|BR|Hill Herd Renamed|I",
        "01/001/0004|CC9012|BR|Moor Herd|I");

    /// <summary>Snapshot mode: the newest file, verbatim. The earlier files leave no trace.</summary>
    public static (string Cphh, string Herdmark, string HerdName)[] ExpectedHerd =>
    [
        ("01/001/0001", "AA1234", "Hill Herd Renamed"),
        ("01/001/0004", "CC9012", "Moor Herd")
    ];

    // sam_party. Single-column key, the degenerate case worth covering explicitly.

    public const string PartyHeader = "PARTY_ID|PARTY_NAME|CHANGE_TYPE";

    private static string PartyFirst => Psv(PartyHeader,
        "P0000001|Alice Holder|I",
        "P0000002|Bob Holder|I");

    private static string PartySecond => Psv(PartyHeader,
        "P0000001|Alice Holder|I",
        "P0000002|Bob Holder|I",
        "P0000004|Superseded Holder|I");

    private static string PartyThird => Psv(PartyHeader,
        "P0000001|Alice Renamed|I",
        "P0000003|Carol Holder|I");

    /// <summary>Snapshot mode: the newest file, verbatim.</summary>
    public static (string PartyId, string PartyName)[] ExpectedParty =>
    [
        ("P0000001", "Alice Renamed"),
        ("P0000003", "Carol Holder")
    ];

    private static string Psv(string header, params string[] rows)
    {
        var builder = new StringBuilder(header).AppendLine();

        foreach (var row in rows)
        {
            builder.AppendLine(row);
        }

        return builder.ToString();
    }
}
