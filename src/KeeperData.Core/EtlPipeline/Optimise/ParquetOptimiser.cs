using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Parquet;
using KeeperData.Core.EtlPipeline.Stages;
using Microsoft.Extensions.Logging;
using Parquet;
using Parquet.Schema;

namespace KeeperData.Core.EtlPipeline.Optimise;

/// <summary>Rewrites one normalised parquet file row-group by row-group into the resolved
/// <see cref="OptimisePlan"/>: projecting columns, converting values to their target types and
/// dropping rows the definition's filter rejects. A group the filter empties entirely is skipped
/// rather than written as a zero-row group.</summary>
public sealed class ParquetOptimiser(DataSetDefinition definition, string fileKey, ILogger logger)
{
    public async Task RunAsync(ParquetReader reader, OptimisePlan plan, Stream destination, CancellationToken cancellationToken)
    {
        var targetFields = plan.KeptColumns.Select(column => column.TargetField!).ToArray();

        await using var writer = await ParquetWriter.CreateAsync(new ParquetSchema(targetFields), destination, cancellationToken: cancellationToken);

        long recordNumber = 0;
        long rowsWritten = 0;

        for (var group = 0; group < reader.RowGroupCount; group++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var rowGroup = reader.OpenRowGroupReader(group);
            var rowCount = (int)rowGroup.RowCount;

            var sourceValues = await ReadColumnsAsync(rowGroup, plan, rowCount, cancellationToken);
            var converted = ConvertKeptColumns(plan, sourceValues, rowCount, recordNumber);
            var mask = BuildMask(plan, sourceValues, converted, rowCount);

            rowsWritten += await WriteGroupAsync(writer, plan, converted, mask, rowCount, cancellationToken);
            recordNumber += rowCount;
        }

        logger.LogInformation(
            "Optimised {FileKey} for dataset {DataSet}: {RowsWritten} of {RowsRead} row(s) written",
            fileKey, definition.Name, rowsWritten, recordNumber);
    }

    /// <summary>Every kept column, plus every column when a filter needs the whole record.</summary>
    private async Task<string?[][]> ReadColumnsAsync(
        ParquetRowGroupReader rowGroup, OptimisePlan plan, int rowCount, CancellationToken cancellationToken)
    {
        var readAll = definition.RowFilter is not null;
        var values = new string?[plan.Columns.Count][];

        foreach (var column in plan.Columns)
        {
            if (!column.Kept && !readAll) continue;

            var data = await ParquetColumns.ReadAsync(rowGroup, column.Source, cancellationToken);
            var strings = new string?[rowCount];

            for (var row = 0; row < data.Length; row++)
            {
                strings[row] = ParquetValueText.Format(data.GetValue(row));
            }

            values[column.SourceIndex] = strings;
        }

        return values;
    }

    /// <summary>The kept columns' values converted to their target types (null for a dropped column).</summary>
    private Array?[] ConvertKeptColumns(OptimisePlan plan, string?[][] sourceValues, int rowCount, long recordNumberBase)
    {
        var converted = new Array?[plan.Columns.Count];

        foreach (var column in plan.KeptColumns)
        {
            var values = Array.CreateInstance(ParquetColumns.ElementType(column.TargetField!), rowCount);

            for (var row = 0; row < rowCount; row++)
            {
                values.SetValue(Convert(column, sourceValues[column.SourceIndex][row], recordNumberBase + row + 1), row);
            }

            converted[column.SourceIndex] = values;
        }

        return converted;
    }

    /// <summary>Which rows the filter keeps, or null when there is no filter.</summary>
    private bool[]? BuildMask(OptimisePlan plan, string?[][] sourceValues, Array?[] converted, int rowCount)
    {
        var filter = definition.RowFilter;

        if (filter is null) return null;

        var mask = new bool[rowCount];

        for (var row = 0; row < rowCount; row++)
        {
            mask[row] = filter(Record(plan, sourceValues, converted, row));
        }

        return mask;
    }

    private static IReadOnlyDictionary<string, object?> Record(OptimisePlan plan, string?[][] sourceValues, Array?[] converted, int row)
    {
        var record = new Dictionary<string, object?>(plan.Columns.Count, StringComparer.OrdinalIgnoreCase);

        foreach (var column in plan.Columns)
        {
            record[column.Source.Name] = column.Kept
                ? converted[column.SourceIndex]!.GetValue(row)
                : sourceValues[column.SourceIndex]?[row];
        }

        return record;
    }

    private static async Task<long> WriteGroupAsync(
        ParquetWriter writer,
        OptimisePlan plan,
        Array?[] converted,
        bool[]? mask,
        int rowCount,
        CancellationToken cancellationToken)
    {
        var kept = mask is null ? rowCount : mask.Count(bit => bit);

        if (kept == 0) return 0;

        using var groupWriter = writer.CreateRowGroup();

        foreach (var column in plan.KeptColumns)
        {
            var values = converted[column.SourceIndex]!;

            if (mask is not null)
            {
                var masked = Array.CreateInstance(values.GetType().GetElementType()!, kept);
                var target = 0;

                for (var row = 0; row < rowCount; row++)
                {
                    if (mask[row])
                    {
                        masked.SetValue(values.GetValue(row), target++);
                    }
                }

                values = masked;
            }

            await ParquetColumns.WriteAsync(groupWriter, column.TargetField!, values, cancellationToken);
        }

        return kept;
    }

    private object? Convert(ColumnResolution column, string? value, long recordNumber)
    {
        try
        {
            return ColumnValueParser.Parse(value, column.TargetType, definition.DecimalPrecision, definition.DecimalScale);
        }
        catch (Exception exception) when (exception is FormatException or OverflowException or InvalidOperationException)
        {
            throw new SourceFileConversionException(
                fileKey, definition.Name, column.Source.Name, column.TargetType.ToString(), value, recordNumber, exception);
        }
    }
}
