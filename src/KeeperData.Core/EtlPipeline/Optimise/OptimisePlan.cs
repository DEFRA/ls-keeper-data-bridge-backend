using KeeperData.Core.ETL.Impl;
using Parquet.Schema;

namespace KeeperData.Core.EtlPipeline.Optimise;

/// <summary>How one source column resolves in the optimised file: whether it is kept, and the type
/// it is written as. <see cref="TargetField"/> is null for a column the projection drops.</summary>
public sealed record ColumnResolution(int SourceIndex, DataField Source, bool Kept, bool MergeRequired, ColumnDataType TargetType, DataField? TargetField);

/// <summary>What the optimise stage does to one file: which columns survive the projection, and
/// the type each surviving column is written as. Merge-required columns are always kept as strings.</summary>
public sealed class OptimisePlan
{
    private OptimisePlan(ColumnResolution[] columns)
    {
        Columns = columns;
        KeptColumns = [.. columns.Where(column => column.Kept)];
        IsIdentity = columns.All(column => column.Kept && column.TargetType == ColumnDataType.String);
    }

    /// <summary>One resolution per source field, in source order.</summary>
    public IReadOnlyList<ColumnResolution> Columns { get; }

    /// <summary>The kept subset of <see cref="Columns"/>, in source order.</summary>
    public IReadOnlyList<ColumnResolution> KeptColumns { get; }

    /// <summary>True when the plan changes nothing: every column kept and every column a string.</summary>
    public bool IsIdentity { get; }

    /// <summary>Definition-level checks that do not depend on a file's schema.</summary>
    public static void ValidateDefinition(DataSetDefinition definition)
    {
        if (definition.IncludedColumns is { Length: > 0 } && definition.ExcludedColumns is { Length: > 0 })
        {
            throw new InvalidOperationException(
                $"Dataset '{definition.Name}' declares both IncludedColumns and ExcludedColumns; they are mutually exclusive.");
        }

        if (definition.ColumnTypes is { Count: > 0 } types)
        {
            var required = MergeRequiredColumns(definition);
            var conflict = types.Keys.FirstOrDefault(required.Contains);

            if (conflict is not null)
            {
                throw new InvalidOperationException(
                    $"Dataset '{definition.Name}' declares a column type for '{conflict}', which the merge requires to stay a string.");
            }
        }
    }

    /// <summary>The columns the merge cannot work without, always retained regardless of include/exclude.</summary>
    public static IReadOnlySet<string> MergeRequiredColumns(DataSetDefinition definition)
    {
        var required = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var name in definition.PrimaryKeyHeaderNames)
        {
            required.Add(name);
        }

        required.Add(definition.ChangeTypeHeaderName);

        if (definition.Audit is not null)
        {
            required.Add(definition.Audit.SequenceColumn);
        }

        return required;
    }

    /// <summary>Resolves the plan for one file's schema.</summary>
    public static OptimisePlan Resolve(
        DataSetDefinition definition,
        DataField[] sourceFields,
        IReadOnlyDictionary<string, ColumnDataType>? detectedTypes)
    {
        var required = MergeRequiredColumns(definition);
        var included = definition.IncludedColumns is null
            ? null
            : new HashSet<string>(definition.IncludedColumns, StringComparer.OrdinalIgnoreCase);
        var excluded = new HashSet<string>(definition.ExcludedColumns, StringComparer.OrdinalIgnoreCase);

        var columns = sourceFields.Select((field, index) =>
        {
            var isRequired = required.Contains(field.Name);
            var kept = isRequired || ((included is null || included.Contains(field.Name)) && !excluded.Contains(field.Name));
            var type = ResolveType(definition, field.Name, isRequired, detectedTypes);

            return new ColumnResolution(index, field, kept, isRequired, type, kept ? TargetFieldFor(definition, field.Name, type) : null);
        }).ToArray();

        return new OptimisePlan(columns);
    }

    private static ColumnDataType ResolveType(
        DataSetDefinition definition,
        string columnName,
        bool mergeRequired,
        IReadOnlyDictionary<string, ColumnDataType>? detectedTypes)
    {
        if (mergeRequired)
        {
            return ColumnDataType.String;
        }

        if (definition.ColumnTypes is not null
            && definition.ColumnTypes.TryGetValue(columnName, out var declared))
        {
            return declared;
        }

        if (definition.AutoDetectColumnTypes
            && detectedTypes is not null
            && detectedTypes.TryGetValue(columnName, out var detected))
        {
            return detected;
        }

        return ColumnDataType.String;
    }

    /// <summary>The parquet field a resolved column is written with; always nullable.</summary>
    private static DataField TargetFieldFor(DataSetDefinition definition, string name, ColumnDataType type)
        => type switch
        {
            ColumnDataType.Int64 => new DataField<long?>(name),
            ColumnDataType.Double => new DataField<double?>(name),
            ColumnDataType.Boolean => new DataField<bool?>(name),
            ColumnDataType.Date => new DataField<DateOnly?>(name),
            ColumnDataType.Timestamp => new DataField<DateTime?>(name),
            ColumnDataType.Decimal => new DecimalDataField(name, definition.DecimalPrecision, definition.DecimalScale, isNullable: true),
            _ => new DataField<string?>(name)
        };
}
