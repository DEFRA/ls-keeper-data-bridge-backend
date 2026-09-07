using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Storage;

namespace KeeperData.Tests.SharedFixtures;

/// <summary>
/// The CTS location identifiers source files, cut down from the five-day sample extract: the keys,
/// the column sets, the record shape, the CRLF line endings and the rows themselves are the sample's,
/// so what the pipeline is asked to read here is what it will meet in production.
///
/// Three things about the feed the shapes below are worth reading for, because each one is load
/// bearing further down:
///
/// The bulk file and the deltas do not carry the same columns. A bulk row is a state, so it has no
/// audit lane and no change type at all; a delta row describes a change, so it carries
/// <c>LID_AUD_ID</c>, <c>LID_AUD_TYPE</c> and <c>LID_AUD_DATETIME</c> ahead of the row. Since all
/// three are excluded from the snapshot, the two files reduce to the same shape once merged.
///
/// <c>RECORD_TYPE</c> is <c>D</c> on every row of both lanes - it says "data record", not "delete" -
/// and <c>RECORD_COUNT</c> restarts at 1 in every file. The change type is <c>LID_AUD_TYPE</c>, and
/// both counters are excluded, which is what keeps a whole file of <c>RECORD_TYPE=D</c> from reading
/// as a whole file of deletes.
///
/// The bulk file is named for the table alone, while a delta carries the run that produced it -
/// <c>CTSM_CADS_PROD_DELTA_00002_001_</c> - ahead of the table name. That, rather than the folder, is
/// what tells the baseline apart once a normalised key has dropped the lane it arrived in.
///
/// Two rows are not the sample's, because the sample cannot supply them: it carries no delete and no
/// unrecognised audit type in its five days, and the extract owner has confirmed both occur. They are
/// marked where they appear.
/// </summary>
public static class CtsFixtures
{
    public static readonly DataSetDefinition Definition =
        StandardDataSetDefinitionsBuilder.Build().CtsLocationIdentifiers!;

    public const string BulkTimestamp = "2026-08-22-072826";

    public const string FirstDeltaTimestamp = "2026-08-23-063010";
    public const string SecondDeltaTimestamp = "2026-08-24-063014";
    public const string ThirdDeltaTimestamp = "2026-08-25-063014";
    public const string FourthDeltaTimestamp = "2026-08-26-063012";
    public const string FifthDeltaTimestamp = "2026-08-27-063014";

    /// <summary>A bulk part cut after some of the deltas, for the replay the reset has to discard.</summary>
    public const string LateBulkTimestamp = "2026-08-26-120000";

    /// <summary>Late enough on the last delta's day that a 30-day lookback sees every file.</summary>
    public static readonly DateTimeOffset RunClock = new(2026, 8, 27, 18, 0, 0, TimeSpan.Zero);

    // The sample's own keys. The bulk carries no run number; a second part therefore has to carry one,
    // and it has to sit ahead of the timestamp, because the timestamp is read from the last segment.
    public const string BulkPartOneKey = $"cads/cts/bulk/CT_LOCATION_IDENTIFIERS_{BulkTimestamp}.xsvn.csv";
    public const string BulkPartTwoKey = $"cads/cts/bulk/CT_LOCATION_IDENTIFIERS_002_{BulkTimestamp}.xsvn.csv";
    public const string LateBulkPartTwoKey = $"cads/cts/bulk/CT_LOCATION_IDENTIFIERS_002_{LateBulkTimestamp}.xsvn.csv";

    public const string FirstDeltaKey = $"cads/cts/daily/CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_{FirstDeltaTimestamp}.xsvn.csv";
    public const string SecondDeltaKey = $"cads/cts/daily/CTSM_CADS_PROD_DELTA_00004_001_CT_LOCATION_IDENTIFIERS_{SecondDeltaTimestamp}.xsvn.csv";
    public const string ThirdDeltaKey = $"cads/cts/daily/CTSM_CADS_PROD_DELTA_00005_001_CT_LOCATION_IDENTIFIERS_{ThirdDeltaTimestamp}.xsvn.csv";
    public const string FourthDeltaKey = $"cads/cts/daily/CTSM_CADS_PROD_DELTA_00006_001_CT_LOCATION_IDENTIFIERS_{FourthDeltaTimestamp}.xsvn.csv";
    public const string FifthDeltaKey = $"cads/cts/daily/CTSM_CADS_PROD_DELTA_00007_001_CT_LOCATION_IDENTIFIERS_{FifthDeltaTimestamp}.xsvn.csv";

    /// <summary>A sibling table sharing both lanes: the extract produces around forty of them, and
    /// discovery has to come back with none.</summary>
    public const string SiblingTableKey =
        $"cads/cts/daily/CTSM_CADS_PROD_DELTA_00006_001_CT_ADDRESSES_{FourthDeltaTimestamp}.xsvn.csv";

    public static readonly DateTimeOffset BulkSourceTimestamp = Timestamp(BulkTimestamp);
    public static readonly DateTimeOffset FourthDeltaSourceTimestamp = Timestamp(FourthDeltaTimestamp);
    public static readonly DateTimeOffset FifthDeltaSourceTimestamp = Timestamp(FifthDeltaTimestamp);
    public static readonly DateTimeOffset FirstDeltaSourceTimestamp = Timestamp(FirstDeltaTimestamp);
    public static readonly DateTimeOffset SecondDeltaSourceTimestamp = Timestamp(SecondDeltaTimestamp);
    public static readonly DateTimeOffset ThirdDeltaSourceTimestamp = Timestamp(ThirdDeltaTimestamp);
    public static readonly DateTimeOffset LateBulkSourceTimestamp = Timestamp(LateBulkTimestamp);

    private const string BulkHeader =
        "RECORD_TYPE,RECORD_COUNT,LID_ID,LID_LOC_ID,LID_EFFECTIVE_FROM_DATE,LID_IDENTIFIER,LID_FULL_IDENTIFIER," +
        "LID_SUB_IDENTIFIER,LID_EFFECTIVE_TO_DATE,LID_CURRENT_STATUS,LID_CURRENT_MODIFIED_DATE,LID_CURRENT_USER," +
        "LID_CURRENT_PID,LID_CURRENT_AMEND_REASON,LID_VERSION";

    private const string DeltaHeader =
        "RECORD_TYPE,RECORD_COUNT,LID_AUD_ID,LID_AUD_TYPE,LID_AUD_DATETIME,LID_ID,LID_LOC_ID,LID_EFFECTIVE_FROM_DATE," +
        "LID_IDENTIFIER,LID_FULL_IDENTIFIER,LID_SUB_IDENTIFIER,LID_EFFECTIVE_TO_DATE,LID_CURRENT_STATUS," +
        "LID_CURRENT_MODIFIED_DATE,LID_CURRENT_USER,LID_CURRENT_PID,LID_CURRENT_AMEND_REASON,LID_VERSION";

    /// <summary>What the snapshot carries: the bulk file's columns, less the two per-file counters.
    /// The audit lane never appears, because only the deltas carry it and all of it is excluded.</summary>
    public static readonly string[] SnapshotColumns =
    [
        "LID_ID", "LID_LOC_ID", "LID_EFFECTIVE_FROM_DATE", "LID_IDENTIFIER", "LID_FULL_IDENTIFIER",
        "LID_SUB_IDENTIFIER", "LID_EFFECTIVE_TO_DATE", "LID_CURRENT_STATUS", "LID_CURRENT_MODIFIED_DATE",
        "LID_CURRENT_USER", "LID_CURRENT_PID", "LID_CURRENT_AMEND_REASON", "LID_VERSION"
    ];

    /// <summary>The four columns the assertions read: the key, something to recognise the row by, and
    /// the two the deltas move.</summary>
    public static readonly string[] AssertedColumns =
        ["LID_ID", "LID_FULL_IDENTIFIER", "LID_CURRENT_MODIFIED_DATE", "LID_VERSION"];

    /// <summary>The baseline. 898949 is deleted by a later delta, 60423 and 287594 are updated by
    /// them, and the other two are only ever carried.</summary>
    public static string BulkPartOne => Records(BulkHeader,
        "D,1,898949,1414957,11-SEP-02,31/124/0042,AH-31/124/0042,,,1,11-SEP-02,m167623,29,,1",
        "D,2,125602,126127,01-JUL-96,21/173/0011,AH-21/173/0011,,,1,20-OCT-09,f800702,232,,1",
        "D,3,171094,171840,01-JUL-96,32/199/9003,AH-32/199/9003,,,2,20-OCT-09,f800702,232,,1",
        "D,4,60423,60768,01-JUL-96,10/325/0068,AH-10/325/0068,,,1,20-OCT-09,f800702,232,,1",
        "D,5,287594,288873,01-JUL-96,55/437/0047,AH-55/437/0047,,,1,20-OCT-09,f800702,232,,1");

    /// <summary>A second part of the same cut, carrying a row part 001 does not. Splits are confirmed
    /// to happen; what the parts are named is not, so this follows the one rule the naming has to
    /// obey - the timestamp stays in the last segment.</summary>
    public static string BulkPartTwo => Records(BulkHeader,
        "D,1,307566,308856,01-JUL-96,75/306/0062,AH-75/306/0062,,,1,20-OCT-09,f800702,232,,1");

    /// <summary>Header only, as two of the sample's five deltas are: nothing changed that day.</summary>
    public static string FirstDelta => Records(DeltaHeader);

    public static string SecondDelta => Records(DeltaHeader);

    public static string ThirdDelta => Records(DeltaHeader,
        "D,1,701762,I,24-AUG-2026 07:55:00,11867589,25940883,24-AUG-26,31/534/5307,AH-31/534/5307,,,1,24-AUG-26,m169953,29,,1",
        "D,2,701782,I,24-AUG-2026 09:26:49,11867590,25940887,24-AUG-26,10/169/0375,AH-10/169/0375,,,1,24-AUG-26,m168941,29,,1");

    /// <summary>An insert and an update from the sample, then the two rows it cannot supply: a delete,
    /// and a row whose audit type is none of the three we know. The X row names a key the previous
    /// delta inserted, so rejecting it rather than guessing at it is visible in the result - the row
    /// stays as inserted.</summary>
    public static string FourthDelta => Records(DeltaHeader,
        "D,1,701862,I,25-AUG-2026 07:33:53,11867604,25940933,25-AUG-26,55/018/8004,AH-55/018/8004,,,1,25-AUG-26,m168941,29,,1",
        "D,2,701882,U,25-AUG-2026 10:51:42,60423,60768,01-JUL-96,10/325/0068,AH-10/325/0068,,,1,25-AUG-26,m169953,29,,1",
        "D,3,701883,D,25-AUG-2026 11:04:07,898949,1414957,11-SEP-02,31/124/0042,AH-31/124/0042,,,1,25-AUG-26,m169953,29,,1",
        "D,4,701884,X,25-AUG-2026 11:22:18,11867589,25940883,24-AUG-26,31/534/5307,AH-31/534/5307,,,9,25-AUG-26,m169953,29,,9");

    /// <summary>The sample's twice-updated key, with the two cuts swapped so the file order and the
    /// sequence disagree: 701929 is written first and must still win. In the sample the two rows are
    /// identical bar the audit lane, so LID_VERSION is carried forward here to make which one won
    /// observable at all.</summary>
    public static string FifthDelta => Records(DeltaHeader,
        "D,1,701929,U,26-AUG-2026 11:32:43,287594,288873,01-JUL-96,55/437/0047,AH-55/437/0047,,,1,26-AUG-26,m174980,29,,2",
        "D,2,701928,U,26-AUG-2026 11:31:51,287594,288873,01-JUL-96,55/437/0047,AH-55/437/0047,,,1,26-AUG-26,m174980,29,,1");

    /// <summary>Part 001 alone, before any delta carrying rows has been applied.</summary>
    public static (string Id, string Identifier, string Modified, string Version)[] ExpectedBulkOnly =>
    [
        ("898949", "AH-31/124/0042", "11-SEP-02", "1"),
        ("125602", "AH-21/173/0011", "20-OCT-09", "1"),
        ("171094", "AH-32/199/9003", "20-OCT-09", "1"),
        ("60423", "AH-10/325/0068", "20-OCT-09", "1"),
        ("287594", "AH-55/437/0047", "20-OCT-09", "1")
    ];

    /// <summary>Both parts and no delta at all: what a reset from a baseline cut after the deltas
    /// leaves behind, since none of them may replay onto it.</summary>
    public static (string Id, string Identifier, string Modified, string Version)[] ExpectedBothBulksOnly =>
    [
        .. ExpectedBulkOnly,
        ("307566", "AH-75/306/0062", "20-OCT-09", "1")
    ];

    /// <summary>After part 001 and the deltas up to and including the fourth: 898949 deleted, 60423
    /// updated, the X row rejected so 11867589 stands as inserted, and 287594 still as the baseline
    /// carried it.</summary>
    public static (string Id, string Identifier, string Modified, string Version)[] ExpectedSteady =>
    [
        ("125602", "AH-21/173/0011", "20-OCT-09", "1"),
        ("171094", "AH-32/199/9003", "20-OCT-09", "1"),
        ("60423", "AH-10/325/0068", "25-AUG-26", "1"),
        ("287594", "AH-55/437/0047", "20-OCT-09", "1"),
        ("11867589", "AH-31/534/5307", "24-AUG-26", "1"),
        ("11867590", "AH-10/169/0375", "24-AUG-26", "1"),
        ("11867604", "AH-55/018/8004", "25-AUG-26", "1")
    ];

    /// <summary>And after the fifth delta, 287594 at the higher of its two sequences.</summary>
    public static (string Id, string Identifier, string Modified, string Version)[] ExpectedSteadyAdvanced =>
    [
        .. ExpectedSteady.Where(row => row.Id != "287594"),
        ("287594", "AH-55/437/0047", "26-AUG-26", "2")
    ];

    /// <summary>And with both bulk parts, the row part 002 carries as well.</summary>
    public static (string Id, string Identifier, string Modified, string Version)[] ExpectedReset =>
    [
        .. ExpectedSteadyAdvanced,
        ("307566", "AH-75/306/0062", "20-OCT-09", "1")
    ];

    /// <summary>The key the fourth delta deletes.</summary>
    public const string DeletedKey = "898949";

    /// <summary>The key the fourth delta's unrecognised audit type names, which the third inserted.</summary>
    public const string RejectedKey = "11867589";

    /// <summary>The hash of a baseline set, over the normalised keys the snapshot stage lists rather
    /// than the source keys, because normalisation drops the lane folder.</summary>
    public static string BaselineHashOf(params string[] sourceKeys)
        => BaselineHash.Compute(sourceKeys.Select(NormalisedKeyOf));

    /// <summary>Where a source file lands once normalised. The compound extension leaves the
    /// <c>.xsvn</c> behind, exactly as the normalise stage does.</summary>
    public static string NormalisedKeyOf(string sourceKey)
        => $"{SnapshotFileNaming.DataSetPrefix(Definition)}{Path.GetFileNameWithoutExtension(sourceKey)}{SnapshotFileNaming.ParquetExtension}";

    public static string SnapshotKeyFor(DateTimeOffset timestamp, string baselineHash)
        => SnapshotFileNaming.SnapshotKey(Definition, timestamp, baselineHash);

    private static DateTimeOffset Timestamp(string value)
        => DataSetFileNaming.ExtractTimestamp(Definition, $"CT_LOCATION_IDENTIFIERS_{value}.xsvn.csv");

    /// <summary>The header record naming the columns, then one data record per row, CRLF terminated as
    /// the extract writes them. There is no trailer: the sample carries none.</summary>
    private static string Records(string header, params string[] rows)
        => string.Join("\r\n", [header, .. rows, string.Empty]);
}
