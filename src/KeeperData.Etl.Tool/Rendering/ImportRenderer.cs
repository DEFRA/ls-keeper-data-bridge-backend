using KeeperData.Core.EtlPipeline.Status;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace KeeperData.Etl.Tool.Rendering;

/// <summary>Writes imports to the console. Report output goes to stdout directly rather than
/// through the logger, so it can be piped without the log furniture around it.</summary>
internal static class ImportRenderer
{
    private static readonly JsonSerializerOptions s_json = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static void WriteJson<T>(T value) => Console.WriteLine(JsonSerializer.Serialize(value, s_json));

    public static void WriteList(EtlImportPage page, int skip)
    {
        if (page.Imports.Count == 0)
        {
            Console.WriteLine("No ETL operations recorded.");
            return;
        }

        Console.WriteLine($"{"IMPORT ID",-38} {"STATUS",-10} {"REQUESTED (UTC)",-20} {"DURATION",10}  DATASET");
        Console.WriteLine(new string('-', 110));

        foreach (var import in page.Imports)
        {
            Console.WriteLine(string.Join(' ',
                $"{import.ImportId,-38}",
                $"{import.Status,-10}",
                $"{Stamp(import.RequestedAtUtc),-20}",
                $"{Duration(import),10} ",
                import.Dataset ?? "(all)"));
        }

        Console.WriteLine();
        Console.WriteLine($"Showing {page.Imports.Count} of {page.TotalCount}, from offset {skip}.");
    }

    public static void WriteDetail(EtlImportDocument import)
    {
        Console.WriteLine($"Import      : {import.ImportId}");
        Console.WriteLine($"Status      : {import.Status}");
        Console.WriteLine($"Source type : {import.SourceType}");
        Console.WriteLine($"Dataset     : {import.Dataset ?? "(all)"}");
        Console.WriteLine($"Requested   : {Stamp(import.RequestedAtUtc)}");
        Console.WriteLine($"Started     : {Stamp(import.StartedAtUtc)}");
        Console.WriteLine($"Completed   : {Stamp(import.CompletedAtUtc)}");
        Console.WriteLine($"Duration    : {Duration(import)}");

        if (import.CurrentStage is { Length: > 0 })
        {
            Console.WriteLine($"Current     : {import.CurrentStage}");
        }

        WriteStages(import);
        WriteDatasets(import);
        WriteOutputs(import);
        WritePurge(import);
        WriteError(import);
    }

    public static void WriteError(EtlImportDocument import)
    {
        if (import.Error is null && import.ErrorDetail is null) return;

        Console.WriteLine();
        Console.WriteLine("Error");
        Console.WriteLine(new string('-', 110));
        Console.WriteLine(import.Error ?? "(no message recorded)");

        if (import.ErrorDetail is not { } detail) return;

        Console.WriteLine();
        WriteIfPresent("Type", detail.Type);
        WriteIfPresent("Stage", detail.Stage);
        WriteIfPresent("Dataset", detail.Dataset);
        WriteIfPresent("File", detail.FileKey);
        WriteIfPresent("Record", detail.RecordNumber?.ToString(CultureInfo.InvariantCulture));
        WriteIfPresent("Expected", detail.Expected);
        WriteIfPresent("Actual", detail.Actual);
    }

    private static void WriteStages(EtlImportDocument import)
    {
        if (import.Stages.Count == 0) return;

        Console.WriteLine();
        Console.WriteLine($"{"STAGE",-22} {"ITEMS",8} {"ELAPSED",12}  COMPLETED (UTC)");
        Console.WriteLine(new string('-', 110));

        foreach (var stage in import.Stages)
        {
            Console.WriteLine($"{stage.Name,-22} {stage.ItemCount,8} {stage.ElapsedMs + "ms",12}  {Stamp(stage.CompletedAtUtc)}");
        }
    }

    private static void WriteDatasets(EtlImportDocument import)
    {
        if (import.Datasets.Count == 0) return;

        Console.WriteLine();
        Console.WriteLine($"{"DATASET",-28} {"SRC",5} {"RAW",5} {"NORM",5} {"OPT",5} {"ROWS",10} {"UPSERT",10} {"DELETE",8} {"REJECT",8}");
        Console.WriteLine(new string('-', 110));

        foreach (var dataset in import.Datasets)
        {
            Console.WriteLine(string.Join(' ',
                $"{dataset.Dataset,-28}",
                $"{dataset.SourceFiles.Count,5}",
                $"{dataset.RawKeys.Count,5}",
                $"{dataset.NormalisedKeys.Count,5}",
                $"{dataset.OptimisedKeys.Count,5}",
                $"{dataset.RowCount?.ToString("N0", CultureInfo.InvariantCulture) ?? "-",10}",
                $"{dataset.RowsUpserted?.ToString("N0", CultureInfo.InvariantCulture) ?? "-",10}",
                $"{dataset.RowsDeleted?.ToString("N0", CultureInfo.InvariantCulture) ?? "-",8}",
                $"{dataset.RowsRejected?.ToString("N0", CultureInfo.InvariantCulture) ?? "-",8}"));

            foreach (var column in dataset.ColumnsNullified)
            {
                Console.WriteLine($"  column nullified: {column}");
            }

            foreach (var column in dataset.ColumnsAdded)
            {
                Console.WriteLine($"  column added    : {column}");
            }
        }
    }

    private static void WriteOutputs(EtlImportDocument import)
    {
        if (import.DuckDbKey is null && import.SqliteKey is null) return;

        Console.WriteLine();
        WriteIfPresent("DuckDB", import.DuckDbKey);
        WriteIfPresent("SQLite", import.SqliteKey);

        foreach (var table in import.SqliteTables)
        {
            Console.WriteLine($"  {table.Name,-28} {table.RowCount,12:N0}");
        }
    }

    private static void WritePurge(EtlImportDocument import)
    {
        if (import.Purge is not { } purge) return;

        Console.WriteLine();
        Console.WriteLine($"Purged stages : {string.Join(", ", purge.Stages)}");
        Console.WriteLine($"Objects deleted: {purge.DeletedCount}");
    }

    private static void WriteIfPresent(string label, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            Console.WriteLine($"{label,-12}: {value}");
        }
    }

    private static string Duration(EtlImportDocument import)
    {
        if (import.StartedAtUtc is not { } started) return "-";

        var finished = import.CompletedAtUtc ?? started;

        return $"{(finished - started).TotalSeconds:N1}s";
    }

    private static string Stamp(DateTime? value)
        => value?.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) ?? "-";
}
