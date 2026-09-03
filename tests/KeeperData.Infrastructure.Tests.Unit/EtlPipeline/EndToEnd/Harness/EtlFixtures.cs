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
/// The three datasets are chosen to cover the interesting axes: composite keys of four, three and
/// one column. All twelve definitions are currently DataSetIngestionMode.Delta, so every dataset
/// folds its files rather than replacing them.
///
/// Two pipeline behaviours shape what the fixtures can assert, and both are deliberate in the code:
///
/// 1. Delta mode ignores deletes. MergeState treats a D row as counted-and-skipped ("delete
///    processing is out of scope"), so the row it names stays in the snapshot at its previous value.
///    Every expectation below therefore keeps its deleted row. Change these fixtures when delete
///    processing is implemented, not before.
///
/// 2. SnapshotStage still has a non-Delta branch that copies the newest normalised file as-is. No
///    definition uses it today, so nothing here exercises it. If a dataset is ever set back to
///    Snapshot mode, its fixture files must each become a complete statement of the world.
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

    /// <summary>Source object key for a dataset at one of the fixture timestamps, carrying the
    /// folder its definition names.</summary>
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

    // Delta increments, not full snapshots: the files fold onto one another.

    private static string HerdSecond => Psv(HerdHeader,
        "01/001/0001|AA1234|BR|Hill Herd Renamed|U",
        "01/001/0004|CC9012|BR|Moor Herd|I");

    private static string HerdThird => Psv(HerdHeader,
        "01/001/0002|BB5678|DY|Vale Herd|D");

    /// <summary>Folded across the three files. The D row for 01/001/0002 is ignored, so it survives.</summary>
    public static (string Cphh, string Herdmark, string HerdName)[] ExpectedHerd =>
    [
        ("01/001/0001", "AA1234", "Hill Herd Renamed"),
        ("01/001/0002", "BB5678", "Vale Herd"),
        ("01/001/0004", "CC9012", "Moor Herd")
    ];

    // sam_party. Single-column key, the degenerate case worth covering explicitly.

    public const string PartyHeader = "PARTY_ID|PARTY_NAME|CHANGE_TYPE";

    private static string PartyFirst => Psv(PartyHeader,
        "P0000001|Alice Holder|I",
        "P0000002|Bob Holder|I");

    private static string PartySecond => Psv(PartyHeader,
        "P0000001|Alice Renamed|U",
        "P0000003|Carol Holder|I");

    private static string PartyThird => Psv(PartyHeader,
        "P0000002|Bob Holder|D");

    /// <summary>Folded across the three files. The D row for P0000002 is ignored, so it survives.</summary>
    public static (string PartyId, string PartyName)[] ExpectedParty =>
    [
        ("P0000001", "Alice Renamed"),
        ("P0000002", "Bob Holder"),
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
