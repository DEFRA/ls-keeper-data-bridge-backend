using KeeperData.Core.Querying.Models;
using System.Text.RegularExpressions;

namespace KeeperData.Core.Querying.Impl;

/// <summary>
/// Parses OData $orderby expressions into custom SortExpression
/// </summary>
internal class ODataToSortExpressionParser
{
    public SortExpression Parse(string orderByExpression)
    {
        if (string.IsNullOrWhiteSpace(orderByExpression))
        {
            throw new ArgumentException("OrderBy expression cannot be empty", nameof(orderByExpression));
        }

        var sortClauses = orderByExpression
            .Split(',')
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToList();

        if (sortClauses.Count == 0)
        {
            throw new ArgumentException("OrderBy expression must contain at least one field", nameof(orderByExpression));
        }

        if (sortClauses.Count == 1)
        {
            return ParseSingleSort(sortClauses[0]);
        }

        var sortExpressions = sortClauses.Select(ParseSingleSort).ToArray();
        return SortExpression.Combine(sortExpressions);
    }

    private SortExpression ParseSingleSort(string sortClause)
    {
        var match = Regex.Match(sortClause, @"^(\w+)(?:\s+(asc|desc))?$", RegexOptions.IgnoreCase);

        if (!match.Success)
        {
            throw new ArgumentException($"Invalid orderBy clause: '{sortClause}'");
        }

        var fieldName = match.Groups[1].Value;
        var direction = match.Groups[2].Success
            ? match.Groups[2].Value.ToLowerInvariant()
            : "asc";

        return direction == "desc"
            ? SortExpression.Descending(fieldName)
            : SortExpression.Ascending(fieldName);
    }
}
