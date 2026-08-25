using KeeperData.Core.EtlPipeline.Storage;
using Parquet;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd.Harness;

/// <summary>
/// Reads named columns out of a snapshot Parquet file.
///
/// Assertions go through this rather than through the staging database on purpose: reading the
/// snapshot directly means a load-stage defect cannot mask a merge-stage defect, and vice versa.
/// </summary>
public static class SnapshotReader
{
    /// <summary>The named columns of a snapshot, as one string array per row.</summary>
    public static async Task<IReadOnlyList<string?[]>> ReadColumnsAsync(
        InMemoryEtlPipelineHost host,
        string snapshotKey,
        params string[] columnNames)
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(columnNames);

        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Snapshots, snapshotKey, ".parquet");

        try
        {
            await using var file = File.OpenRead(path);
            await using var reader = await ParquetReader.CreateAsync(file);

            var fields = reader.Schema.GetDataFields();
            var selected = columnNames.Select(name =>
            {
                var index = Array.FindIndex(fields, field => field.Name == name);

                return index >= 0
                    ? fields[index]
                    : throw new InvalidOperationException(
                        $"Snapshot '{snapshotKey}' has no column '{name}'. Columns present: {string.Join(", ", fields.Select(f => f.Name))}");
            }).ToArray();

            var rows = new List<string?[]>();

            for (var group = 0; group < reader.RowGroupCount; group++)
            {
                using var rowGroup = reader.OpenRowGroupReader(group);

                var columns = new string?[selected.Length][];

                for (var column = 0; column < selected.Length; column++)
                {
                    columns[column] = new string?[rowGroup.RowCount];
                    await rowGroup.ReadAsync(selected[column], columns[column].AsMemory());
                }

                for (var row = 0; row < rowGroup.RowCount; row++)
                {
                    rows.Add([.. columns.Select(column => column[row])]);
                }
            }

            return rows;
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>The column names a snapshot carries, for asserting on what was dropped.</summary>
    public static async Task<IReadOnlyList<string>> ColumnNamesAsync(
        InMemoryEtlPipelineHost host,
        string snapshotKey)
    {
        ArgumentNullException.ThrowIfNull(host);

        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Snapshots, snapshotKey, ".parquet");

        try
        {
            await using var file = File.OpenRead(path);
            await using var reader = await ParquetReader.CreateAsync(file);

            return [.. reader.Schema.GetDataFields().Select(field => field.Name)];
        }
        finally
        {
            File.Delete(path);
        }
    }
}
