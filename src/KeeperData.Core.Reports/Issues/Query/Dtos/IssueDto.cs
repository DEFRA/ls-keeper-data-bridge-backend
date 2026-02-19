namespace KeeperData.Core.Reports.Issues.Query.Dtos;

public class IssueDto
{
    public required string Id { get; set; }

    /// <summary>
    /// Gets or sets the CTS LID full identifier value.
    /// </summary>
    public required string CtsLidFullIdentifier { get; set; }

    /// <summary>
    /// Gets or sets the CPH value.
    /// </summary>
    public required string Cph { get; set; }

    public required string IssueCode { get; set; }
    public required string RuleCode { get; set; }
    public required string ErrorCode { get; set; }
    public required string ErrorDescription { get; set; }
    public string[]? EmailCTS { get; set; }
    public string? EmailSAM { get; set; }
    public string[]? TelCTS { get; set; }
    public string? TelSAM { get; set; }
    public string? FSA { get; set; }


    /// <summary>
    /// Gets or sets the timestamp when the issue was first detected.
    /// </summary>
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>
    /// Gets or sets the timestamp when the issue was last checked.
    /// </summary>
    public DateTime LastUpdatedAtUtc { get; set; }

    /// <summary>
    /// Gets or sets whether the issue is currently active.
    /// </summary>
    public bool IsActive { get; set; }

    /// <summary>
    /// Gets or sets whether the issue has been flagged as ignored by a user.
    /// </summary>
    public bool IsIgnored { get; set; }

    /// <summary>
    /// Gets or sets the manual resolution workflow status.
    /// </summary>
    public string ResolutionStatus { get; set; } = "None";

    /// <summary>
    /// Gets or sets the user assigned to this issue (null if unassigned).
    /// </summary>
    public string? AssignedTo { get; set; }
}
