using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Issues.Query.Dtos;

/// <summary>
/// Groups filter values for building a <see cref="CleanseIssueQueryDto"/>.
/// Null/empty values are ignored by the query builder.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public sealed record CleanseIssueFilterValues
{
    /// <summary>Filter by active status (null = all).</summary>
    public bool? IsActive { get; init; }

    /// <summary>Filter by CTS LID full identifier (contains).</summary>
    public string? CtsLidFullIdentifier { get; init; }

    /// <summary>Filter by CPH (contains).</summary>
    public string? Cph { get; init; }

    /// <summary>Filter by exact issue code.</summary>
    public string? IssueCode { get; init; }

    /// <summary>Filter by exact rule code.</summary>
    public string? RuleCode { get; init; }

    /// <summary>Filter by exact error code.</summary>
    public string? ErrorCode { get; init; }

    /// <summary>Filter by ignored status (null = all).</summary>
    public bool? IsIgnored { get; init; }

    /// <summary>Filter by resolution status (null = all).</summary>
    public string? ResolutionStatus { get; init; }

    /// <summary>Filter by assigned user (null = all).</summary>
    public string? AssignedTo { get; init; }

    /// <summary>Filter to unassigned issues only when true.</summary>
    public bool? IsUnassigned { get; init; }

    /// <summary>Filter issues created after this time.</summary>
    public DateTime? CreatedAfterUtc { get; init; }

    /// <summary>Filter issues created before this time.</summary>
    public DateTime? CreatedBeforeUtc { get; init; }

    /// <summary>Filter issues updated after this time.</summary>
    public DateTime? UpdatedAfterUtc { get; init; }

    /// <summary>Filter issues updated before this time.</summary>
    public DateTime? UpdatedBeforeUtc { get; init; }
}
