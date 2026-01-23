namespace KeeperData.Core.Reports.Querying;

/// <summary>
/// Fluent query builder for cleanse issues.
/// </summary>
public sealed class CleanseIssueQuery
{
    /// <summary>Filter by active status (null = all).</summary>
    public bool? IsActive { get; private set; }

    /// <summary>Filter by specific issue code.</summary>
    public string? IssueCode { get; private set; }

    /// <summary>Filter by CPH containing this value.</summary>
    public string? CphContains { get; private set; }

    /// <summary>Filter by CPH starting with this value.</summary>
    public string? CphStartsWith { get; private set; }

    /// <summary>Filter issues created after this time.</summary>
    public DateTime? CreatedAfterUtc { get; private set; }

    /// <summary>Filter issues created before this time.</summary>
    public DateTime? CreatedBeforeUtc { get; private set; }

    /// <summary>Filter issues updated after this time.</summary>
    public DateTime? UpdatedAfterUtc { get; private set; }

    /// <summary>Filter issues updated before this time.</summary>
    public DateTime? UpdatedBeforeUtc { get; private set; }

    /// <summary>Field to sort by.</summary>
    public CleanseIssueSortField SortBy { get; private set; } = CleanseIssueSortField.LastUpdatedAtUtc;

    /// <summary>Sort in descending order.</summary>
    public bool SortDescending { get; private set; } = true;

    /// <summary>Number of records to skip.</summary>
    public int Skip { get; private set; }

    /// <summary>Maximum number of records to return.</summary>
    public int Top { get; private set; } = 50;

    private CleanseIssueQuery() { }

    /// <summary>Creates a new query builder.</summary>
    public static CleanseIssueQuery Create() => new();

    /// <summary>Filters to active issues only.</summary>
    public CleanseIssueQuery WhereActive()
    {
        IsActive = true;
        return this;
    }

    /// <summary>Filters to inactive issues only.</summary>
    public CleanseIssueQuery WhereInactive()
    {
        IsActive = false;
        return this;
    }

    /// <summary>Filters by specific issue code.</summary>
    public CleanseIssueQuery WithIssueCode(string code)
    {
        IssueCode = code;
        return this;
    }

    /// <summary>Filters by CPH containing the specified value.</summary>
    public CleanseIssueQuery WithCphContaining(string value)
    {
        CphContains = value;
        return this;
    }

    /// <summary>Filters by CPH starting with the specified value.</summary>
    public CleanseIssueQuery WithCphStartingWith(string value)
    {
        CphStartsWith = value;
        return this;
    }

    /// <summary>Filters issues created after the specified time.</summary>
    public CleanseIssueQuery CreatedAfter(DateTime utcTime)
    {
        CreatedAfterUtc = utcTime;
        return this;
    }

    /// <summary>Filters issues created before the specified time.</summary>
    public CleanseIssueQuery CreatedBefore(DateTime utcTime)
    {
        CreatedBeforeUtc = utcTime;
        return this;
    }

    /// <summary>Filters issues updated (created or reactivated) after the specified time.</summary>
    public CleanseIssueQuery UpdatedAfter(DateTime utcTime)
    {
        UpdatedAfterUtc = utcTime;
        return this;
    }

    /// <summary>Filters issues updated before the specified time.</summary>
    public CleanseIssueQuery UpdatedBefore(DateTime utcTime)
    {
        UpdatedBeforeUtc = utcTime;
        return this;
    }

    /// <summary>Sets the sort field and direction.</summary>
    public CleanseIssueQuery OrderBy(CleanseIssueSortField field, bool descending = false)
    {
        SortBy = field;
        SortDescending = descending;
        return this;
    }

    /// <summary>Sets pagination parameters.</summary>
    public CleanseIssueQuery Page(int skip, int top)
    {
        Skip = skip;
        Top = top;
        return this;
    }
}
