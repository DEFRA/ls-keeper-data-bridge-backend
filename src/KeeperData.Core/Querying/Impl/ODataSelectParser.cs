namespace KeeperData.Core.Querying.Impl;

/// <summary>
/// Parses OData $select expressions into a list of field names.
/// Supports comma-separated field names (e.g., "CPH,UpdatedAtUtc,IsDeleted").
/// </summary>
internal class ODataSelectParser
{
    /// <summary>
    /// Parses a $select expression and returns a list of field names to include.
    /// </summary>
    /// <param name="selectExpression">The OData $select expression (e.g., "CPH,UpdatedAtUtc")</param>
    /// <returns>A list of field names to project, or null if expression is empty</returns>
    /// <exception cref="ArgumentException">Thrown when the expression is invalid</exception>
    public IReadOnlyList<string>? Parse(string? selectExpression)
    {
        if (string.IsNullOrWhiteSpace(selectExpression))
        {
            return null;
        }

        var fields = selectExpression
            .Split(',')
            .Select(f => f.Trim())
            .Where(f => !string.IsNullOrWhiteSpace(f))
            .ToList();

        if (fields.Count == 0)
        {
            throw new ArgumentException("Select expression must contain at least one field", nameof(selectExpression));
        }

        // Validate field names - basic validation for common patterns
        foreach (var field in fields)
        {
            if (!IsValidFieldName(field))
            {
                throw new ArgumentException($"Invalid field name in select expression: '{field}'", nameof(selectExpression));
            }
        }

        return fields;
    }

    private static bool IsValidFieldName(string fieldName)
    {
        // Field names should start with a letter or underscore
        // and contain only letters, digits, underscores, and dots (for nested fields)
        if (string.IsNullOrWhiteSpace(fieldName))
        {
            return false;
        }

        if (!char.IsLetter(fieldName[0]) && fieldName[0] != '_')
        {
            return false;
        }

        return fieldName.All(c => char.IsLetterOrDigit(c) || c == '_' || c == '.');
    }
}