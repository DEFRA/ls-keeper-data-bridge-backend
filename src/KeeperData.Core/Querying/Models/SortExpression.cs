namespace KeeperData.Core.Querying.Models;

/// <summary>
/// Represents a sort expression for ordering query results.
/// Abstracts away the underlying data store implementation.
/// </summary>
public abstract class SortExpression
{
    /// <summary>
    /// Creates an ascending sort by a single field
    /// </summary>
    public static SortExpression Ascending(string fieldName) 
        => new SingleFieldSort(fieldName, SortDirection.Ascending);

    /// <summary>
    /// Creates a descending sort by a single field
    /// </summary>
    public static SortExpression Descending(string fieldName) 
        => new SingleFieldSort(fieldName, SortDirection.Descending);

    /// <summary>
    /// Combines multiple sort expressions (applied in order)
    /// </summary>
    public static SortExpression Combine(params SortExpression[] sorts) 
        => new CompoundSort(sorts);
}

public enum SortDirection
{
    Ascending,
    Descending
}

public sealed class SingleFieldSort : SortExpression
{
    public string FieldName { get; }
    public SortDirection Direction { get; }

    public SingleFieldSort(string fieldName, SortDirection direction)
    {
        FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
        Direction = direction;
    }
}

public sealed class CompoundSort : SortExpression
{
    public IReadOnlyList<SortExpression> Sorts { get; }

    public CompoundSort(params SortExpression[] sorts)
    {
        Sorts = sorts ?? throw new ArgumentNullException(nameof(sorts));
        
        if (Sorts.Count == 0)
        {
            throw new ArgumentException("At least one sort expression is required", nameof(sorts));
        }
    }
}
