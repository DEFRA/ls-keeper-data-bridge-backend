namespace KeeperData.Core.Querying.Models;

/// <summary>
/// Represents a filter expression for querying data.
/// Abstracts away the underlying data store implementation.
/// </summary>
public abstract class FilterExpression
{
    /// <summary>
    /// Creates an equality filter (field == value)
    /// </summary>
    public static FilterExpression Equal(string fieldName, object? value) 
        => new ComparisonFilter(fieldName, ComparisonOperator.Equal, value);

    /// <summary>
    /// Creates a not-equal filter (field != value)
    /// </summary>
    public static FilterExpression NotEqual(string fieldName, object? value) 
        => new ComparisonFilter(fieldName, ComparisonOperator.NotEqual, value);

    /// <summary>
    /// Creates a greater-than filter (field > value)
    /// </summary>
    public static FilterExpression GreaterThan(string fieldName, object value) 
        => new ComparisonFilter(fieldName, ComparisonOperator.GreaterThan, value);

    /// <summary>
    /// Creates a greater-than-or-equal filter (field >= value)
    /// </summary>
    public static FilterExpression GreaterThanOrEqual(string fieldName, object value) 
        => new ComparisonFilter(fieldName, ComparisonOperator.GreaterThanOrEqual, value);

    /// <summary>
    /// Creates a less-than filter (field < value)
    /// </summary>
    public static FilterExpression LessThan(string fieldName, object value) 
        => new ComparisonFilter(fieldName, ComparisonOperator.LessThan, value);

    /// <summary>
    /// Creates a less-than-or-equal filter (field <= value)
    /// </summary>
    public static FilterExpression LessThanOrEqual(string fieldName, object value) 
        => new ComparisonFilter(fieldName, ComparisonOperator.LessThanOrEqual, value);

    /// <summary>
    /// Creates an IN filter (field in [values])
    /// </summary>
    public static FilterExpression In(string fieldName, IEnumerable<object> values) 
        => new InFilter(fieldName, values);

    /// <summary>
    /// Creates a contains filter (field contains value) - case insensitive
    /// </summary>
    public static FilterExpression Contains(string fieldName, string value, bool caseSensitive = false) 
        => new TextFilter(fieldName, TextOperator.Contains, value, caseSensitive);

    /// <summary>
    /// Creates a starts-with filter (field starts with value) - case insensitive by default
    /// </summary>
    public static FilterExpression StartsWith(string fieldName, string value, bool caseSensitive = false) 
        => new TextFilter(fieldName, TextOperator.StartsWith, value, caseSensitive);

    /// <summary>
    /// Creates an ends-with filter (field ends with value) - case insensitive by default
    /// </summary>
    public static FilterExpression EndsWith(string fieldName, string value, bool caseSensitive = false) 
        => new TextFilter(fieldName, TextOperator.EndsWith, value, caseSensitive);

    /// <summary>
    /// Creates a regex filter
    /// </summary>
    public static FilterExpression Regex(string fieldName, string pattern, bool caseSensitive = false) 
        => new RegexFilter(fieldName, pattern, caseSensitive);

    /// <summary>
    /// Creates an exists filter (field exists/is not null)
    /// </summary>
    public static FilterExpression Exists(string fieldName) 
        => new ExistsFilter(fieldName, true);

    /// <summary>
    /// Creates a not-exists filter (field does not exist/is null)
    /// </summary>
    public static FilterExpression NotExists(string fieldName) 
        => new ExistsFilter(fieldName, false);

    /// <summary>
    /// Combines filters with AND logic
    /// </summary>
    public static FilterExpression And(params FilterExpression[] filters) 
        => new LogicalFilter(LogicalOperator.And, filters);

    /// <summary>
    /// Combines filters with OR logic
    /// </summary>
    public static FilterExpression Or(params FilterExpression[] filters) 
        => new LogicalFilter(LogicalOperator.Or, filters);

    /// <summary>
    /// Negates a filter
    /// </summary>
    public static FilterExpression Not(FilterExpression filter) 
        => new NotFilter(filter);

    /// <summary>
    /// Creates an empty filter (matches all documents)
    /// </summary>
    public static FilterExpression Empty() => new EmptyFilter();

    /// <summary>
    /// Returns a string representation of the filter expression.
    /// </summary>
    public abstract override string ToString();
}

public enum ComparisonOperator
{
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual
}

public enum TextOperator
{
    Contains,
    StartsWith,
    EndsWith
}

public enum LogicalOperator
{
    And,
    Or
}

public sealed class ComparisonFilter : FilterExpression
{
    public string FieldName { get; }
    public ComparisonOperator Operator { get; }
    public object? Value { get; }

    public ComparisonFilter(string fieldName, ComparisonOperator @operator, object? value)
    {
        FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
        Operator = @operator;
        Value = value;
    }

    public override string ToString()
    {
        var op = Operator switch
        {
            ComparisonOperator.Equal => "==",
            ComparisonOperator.NotEqual => "!=",
            ComparisonOperator.GreaterThan => ">",
            ComparisonOperator.GreaterThanOrEqual => ">=",
            ComparisonOperator.LessThan => "<",
            ComparisonOperator.LessThanOrEqual => "<=",
            _ => Operator.ToString()
        };
        var valueStr = FormatValue(Value);
        return $"{FieldName} {op} {valueStr}";
    }

    private static string FormatValue(object? value) => value switch
    {
        null => "null",
        string s => $"'{s}'",
        DateTime dt => $"'{dt:O}'",
        DateTimeOffset dto => $"'{dto:O}'",
        bool b => b.ToString().ToLowerInvariant(),
        _ => value.ToString() ?? "null"
    };
}

public sealed class InFilter : FilterExpression
{
    public string FieldName { get; }
    public IReadOnlyList<object> Values { get; }

    public InFilter(string fieldName, IEnumerable<object> values)
    {
        FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
        Values = values?.ToList() ?? throw new ArgumentNullException(nameof(values));
    }

    public override string ToString()
    {
        var valuesStr = string.Join(", ", Values.Select(v => v is string s ? $"'{s}'" : v?.ToString() ?? "null"));
        return $"{FieldName} in [{valuesStr}]";
    }
}

public sealed class TextFilter : FilterExpression
{
    public string FieldName { get; }
    public TextOperator Operator { get; }
    public string Value { get; }
    public bool CaseSensitive { get; }

    public TextFilter(string fieldName, TextOperator @operator, string value, bool caseSensitive)
    {
        FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
        Operator = @operator;
        Value = value ?? throw new ArgumentNullException(nameof(value));
        CaseSensitive = caseSensitive;
    }

    public override string ToString()
    {
        var caseSuffix = CaseSensitive ? "" : " (case-insensitive)";
        return Operator switch
        {
            TextOperator.Contains => $"{FieldName} contains '{Value}'{caseSuffix}",
            TextOperator.StartsWith => $"{FieldName} startsWith '{Value}'{caseSuffix}",
            TextOperator.EndsWith => $"{FieldName} endsWith '{Value}'{caseSuffix}",
            _ => $"{FieldName} {Operator} '{Value}'{caseSuffix}"
        };
    }
}

public sealed class RegexFilter : FilterExpression
{
    public string FieldName { get; }
    public string Pattern { get; }
    public bool CaseSensitive { get; }

    public RegexFilter(string fieldName, string pattern, bool caseSensitive)
    {
        FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
        Pattern = pattern ?? throw new ArgumentNullException(nameof(pattern));
        CaseSensitive = caseSensitive;
    }

    public override string ToString()
    {
        var caseSuffix = CaseSensitive ? "" : " (case-insensitive)";
        return $"{FieldName} matches /{Pattern}/{caseSuffix}";
    }
}

public sealed class ExistsFilter : FilterExpression
{
    public string FieldName { get; }
    public bool ShouldExist { get; }

    public ExistsFilter(string fieldName, bool shouldExist)
    {
        FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
        ShouldExist = shouldExist;
    }

    public override string ToString() 
        => ShouldExist ? $"{FieldName} exists" : $"{FieldName} not exists";
}

public sealed class LogicalFilter : FilterExpression
{
    public LogicalOperator Operator { get; }
    public IReadOnlyList<FilterExpression> Filters { get; }

    public LogicalFilter(LogicalOperator @operator, params FilterExpression[] filters)
    {
        Operator = @operator;
        Filters = filters ?? throw new ArgumentNullException(nameof(filters));
        
        if (Filters.Count == 0)
        {
            throw new ArgumentException("At least one filter is required", nameof(filters));
        }
    }

    public override string ToString()
    {
        var op = Operator == LogicalOperator.And ? " AND " : " OR ";
        var parts = Filters.Select(f => f is LogicalFilter ? $"({f})" : f.ToString());
        return string.Join(op, parts);
    }
}

public sealed class NotFilter : FilterExpression
{
    public FilterExpression Filter { get; }

    public NotFilter(FilterExpression filter)
    {
        Filter = filter ?? throw new ArgumentNullException(nameof(filter));
    }

    public override string ToString() 
        => Filter is LogicalFilter ? $"NOT ({Filter})" : $"NOT {Filter}";
}

public sealed class EmptyFilter : FilterExpression
{
    public override string ToString() => "(empty)";
}
