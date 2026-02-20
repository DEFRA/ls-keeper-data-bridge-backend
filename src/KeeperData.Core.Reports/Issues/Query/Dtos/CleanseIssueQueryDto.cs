namespace KeeperData.Core.Reports.Issues.Query.Dtos;

/// <summary>
/// Fluent query builder for cleanse issues.
/// </summary>
public sealed class CleanseIssueQueryDto
{
    /// <summary>Filter by active status (null = all).</summary>
    public bool? IsActive { get; private set; }

    /// <summary>Filter by CTS LID full identifier containing this value.</summary>
    public string? CtsLidFullIdentifierContains { get; private set; }

    /// <summary>Filter by specific issue code.</summary>
    public string? IssueCode { get; private set; }

    /// <summary>Filter by specific rule code.</summary>
    public string? RuleCode { get; private set; }

    /// <summary>Filter by specific error code.</summary>
    public string? ErrorCode { get; private set; }

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

    /// <summary>Filter by ignored status (null = all).</summary>
    public bool? IsIgnored { get; private set; }

    /// <summary>Filter by resolution status (null = all).</summary>
    public string? ResolutionStatus { get; private set; }

    /// <summary>Filter by assigned user (null = all).</summary>
    public string? AssignedTo { get; private set; }

    /// <summary>Filter to unassigned issues only when true.</summary>
    public bool? IsUnassigned { get; private set; }

    /// <summary>Field to sort by.</summary>
    public CleanseIssueSortField SortBy { get; private set; } = CleanseIssueSortField.LastUpdatedAtUtc;

    /// <summary>Sort in descending order.</summary>
    public bool SortDescending { get; private set; } = true;

    /// <summary>Number of records to skip.</summary>
    public int Skip { get; private set; }

    /// <summary>Maximum number of records to return.</summary>
    public int Top { get; private set; } = 50;

    private CleanseIssueQueryDto() { }

    /// <summary>Creates a new query builder.</summary>
    public static CleanseIssueQueryDto Create() => new();

    /// <summary>Filters to active issues only.</summary>
    public CleanseIssueQueryDto WhereActive()
    {
        IsActive = true;
        return this;
    }

    /// <summary>Filters to inactive issues only.</summary>
    public CleanseIssueQueryDto WhereInactive()
    {
        IsActive = false;
        return this;
    }

    /// <summary>Filters by specific issue code.</summary>
    public CleanseIssueQueryDto WithIssueCode(string code)
    {
        IssueCode = code;
        return this;
    }

    /// <summary>Filters by specific rule code.</summary>
    public CleanseIssueQueryDto WithRuleCode(string code)
    {
        RuleCode = code;
        return this;
    }

    /// <summary>Filters by specific error code.</summary>
    public CleanseIssueQueryDto WithErrorCode(string code)
    {
        ErrorCode = code;
        return this;
    }

    /// <summary>Filters by CTS LID full identifier containing the specified value.</summary>
    public CleanseIssueQueryDto WithCtsLidFullIdentifierContaining(string value)
    {
        CtsLidFullIdentifierContains = value;
        return this;
    }

    /// <summary>Filters by CPH containing the specified value.</summary>
    public CleanseIssueQueryDto WithCphContaining(string value)
    {
        CphContains = value;
        return this;
    }

    /// <summary>Filters by CPH starting with the specified value.</summary>
    public CleanseIssueQueryDto WithCphStartingWith(string value)
    {
        CphStartsWith = value;
        return this;
    }

    /// <summary>Filters issues created after the specified time.</summary>
    public CleanseIssueQueryDto CreatedAfter(DateTime utcTime)
    {
        CreatedAfterUtc = utcTime;
        return this;
    }

    /// <summary>Filters issues created before the specified time.</summary>
    public CleanseIssueQueryDto CreatedBefore(DateTime utcTime)
    {
        CreatedBeforeUtc = utcTime;
        return this;
    }

    /// <summary>Filters issues updated (created or reactivated) after the specified time.</summary>
    public CleanseIssueQueryDto UpdatedAfter(DateTime utcTime)
    {
        UpdatedAfterUtc = utcTime;
        return this;
    }

    /// <summary>Filters issues updated before the specified time.</summary>
    public CleanseIssueQueryDto UpdatedBefore(DateTime utcTime)
    {
        UpdatedBeforeUtc = utcTime;
        return this;
    }

    /// <summary>Filters to ignored issues only.</summary>
    public CleanseIssueQueryDto WhereIgnored()
    {
        IsIgnored = true;
        return this;
    }

    /// <summary>Filters to non-ignored issues only.</summary>
    public CleanseIssueQueryDto WhereNotIgnored()
    {
        IsIgnored = false;
        return this;
    }

    /// <summary>Filters by resolution status.</summary>
    public CleanseIssueQueryDto WithResolutionStatus(string status)
    {
        ResolutionStatus = status;
        return this;
    }

    /// <summary>Filters by assigned user.</summary>
    public CleanseIssueQueryDto WithAssignedTo(string user)
    {
        AssignedTo = user;
        return this;
    }

    /// <summary>Filters to unassigned issues only.</summary>
    public CleanseIssueQueryDto WhereUnassigned()
    {
        IsUnassigned = true;
        return this;
    }

    /// <summary>Sets the sort field and direction.</summary>
    public CleanseIssueQueryDto OrderBy(CleanseIssueSortField field, bool descending = false)
    {
        SortBy = field;
        SortDescending = descending;
        return this;
    }

    /// <summary>Sets pagination parameters.</summary>
    public CleanseIssueQueryDto Page(int skip, int top)
    {
        Skip = skip;
        Top = top;
        return this;
    }

    /// <summary>
    /// Creates a query from sort/pagination parameters and optional filter values.
    /// </summary>
    public static CleanseIssueQueryDto From(
        CleanseIssueSortField sortField, bool sortDescending, int skip, int top,
        CleanseIssueFilterValues? filters = null)
    {
        var f = filters ?? new CleanseIssueFilterValues();
        return new()
        {
            SortBy = sortField,
            SortDescending = sortDescending,
            Skip = skip,
            Top = top,
            IsActive = f.IsActive,
            CtsLidFullIdentifierContains = f.CtsLidFullIdentifier,
            CphContains = f.Cph,
            IssueCode = f.IssueCode,
            RuleCode = f.RuleCode,
            ErrorCode = f.ErrorCode,
            IsIgnored = f.IsIgnored,
            ResolutionStatus = f.ResolutionStatus,
            AssignedTo = f.AssignedTo,
            IsUnassigned = f.IsUnassigned is true ? true : null,
            CreatedAfterUtc = f.CreatedAfterUtc,
            CreatedBeforeUtc = f.CreatedBeforeUtc,
            UpdatedAfterUtc = f.UpdatedAfterUtc,
            UpdatedBeforeUtc = f.UpdatedBeforeUtc
        };
    }
}
