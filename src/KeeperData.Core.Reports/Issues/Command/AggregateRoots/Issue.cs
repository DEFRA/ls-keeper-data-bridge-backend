using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Issues.Command.AggregateRoots;

/// <summary>
/// Aggregate root: Represents a data quality issue detected during cleanse analysis.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Aggregate root - covered by integration tests.")]
public class Issue
{
    /// <summary>
    /// Gets or sets the unique identifier (thumbprint/hash of primary record id + rule id).
    /// </summary>
    public required string Id { get; set; }

    /// <summary>
    /// Gets or sets the identifier of the analysis operation that last touched this issue.
    /// </summary>
    public string OperationId { get; set; } = string.Empty;

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
    /// Gets or sets whether the issue is currently active (rule condition true on latest run).
    /// </summary>
    public bool IsActive { get; set; }

    /// <summary>
    /// Gets or sets whether the issue has been flagged as ignored by a user.
    /// Independent of Active/Inactive status.
    /// </summary>
    public bool IsIgnored { get; set; }

    /// <summary>
    /// Gets or sets the manual resolution workflow status.
    /// </summary>
    public ResolutionStatus ResolutionStatus { get; set; }

    /// <summary>
    /// Gets or sets the user assigned to this issue (null if unassigned).
    /// </summary>
    public string? AssignedTo { get; set; }

    /// <summary>
    /// Creates a new active issue from a rule activation.
    /// </summary>
    public static (Issue Issue, IssueHistoryEntry History) Create(
        string thumbprint,
        string operationId,
        RuleDescriptor descriptor,
        Cph cph,
        string? ctsLidFullIdentifier = null,
        IssueContextData? context = null)
    {
        var now = DateTime.UtcNow;
        var issue = new Issue
        {
            Id = thumbprint,
            OperationId = operationId,
            IssueCode = descriptor.RuleId,
            RuleCode = descriptor.UserRuleNo,
            ErrorCode = descriptor.UserErrorCode,
            ErrorDescription = descriptor.UserDescription,
            CtsLidFullIdentifier = ctsLidFullIdentifier ?? string.Empty,
            Cph = cph.Value,
            CreatedAtUtc = now,
            LastUpdatedAtUtc = now,
            IsActive = true
        };
        issue.ApplyContext(context);

        var history = IssueHistoryEntry.Create(thumbprint, IssueAction.Created, "system", "Issue detected");
        return (issue, history);
    }

    /// <summary>
    /// Reactivates a previously resolved issue.
    /// </summary>
    public IssueHistoryEntry Reactivate(string operationId)
    {
        IsActive = true;
        OperationId = operationId;
        LastUpdatedAtUtc = DateTime.UtcNow;
        return IssueHistoryEntry.Create(Id, IssueAction.Reactivated, "system", "Issue reactivated by analysis");
    }

    /// <summary>
    /// Marks this issue as touched by the current operation (already active).
    /// </summary>
    public IssueHistoryEntry Touch(string operationId)
    {
        OperationId = operationId;
        LastUpdatedAtUtc = DateTime.UtcNow;
        return IssueHistoryEntry.Create(Id, IssueAction.Touched, "system", "Issue confirmed by analysis");
    }

    /// <summary>
    /// Deactivates this issue (rule condition no longer true).
    /// </summary>
    public IssueHistoryEntry Deactivate()
    {
        IsActive = false;
        LastUpdatedAtUtc = DateTime.UtcNow;
        return IssueHistoryEntry.Create(Id, IssueAction.Deactivated, "system", "Issue no longer detected");
    }

    /// <summary>
    /// Flags this issue as ignored.
    /// </summary>
    public IssueHistoryEntry Ignore(string performedBy)
    {
        IsIgnored = true;
        LastUpdatedAtUtc = DateTime.UtcNow;
        return IssueHistoryEntry.Create(Id, IssueAction.Ignored, performedBy);
    }

    /// <summary>
    /// Removes the ignored flag from this issue.
    /// </summary>
    public IssueHistoryEntry Unignore(string performedBy)
    {
        IsIgnored = false;
        LastUpdatedAtUtc = DateTime.UtcNow;
        return IssueHistoryEntry.Create(Id, IssueAction.Unignored, performedBy);
    }

    /// <summary>
    /// Updates the manual resolution workflow status.
    /// </summary>
    public IssueHistoryEntry UpdateResolutionStatus(ResolutionStatus status, string performedBy)
    {
        var previous = ResolutionStatus;
        ResolutionStatus = status;
        LastUpdatedAtUtc = DateTime.UtcNow;
        return IssueHistoryEntry.Create(Id, IssueAction.ResolutionStatusChanged, performedBy,
            $"ResolutionStatus: {previous} → {status}");
    }

    /// <summary>
    /// Assigns this issue to a user.
    /// </summary>
    public IssueHistoryEntry Assign(string assignedTo, string performedBy)
    {
        AssignedTo = assignedTo;
        LastUpdatedAtUtc = DateTime.UtcNow;
        return IssueHistoryEntry.Create(Id, IssueAction.Assigned, performedBy, $"Assigned to {assignedTo}");
    }

    /// <summary>
    /// Clears the assigned user from this issue.
    /// </summary>
    public IssueHistoryEntry Unassign(string performedBy)
    {
        var previous = AssignedTo;
        AssignedTo = null;
        LastUpdatedAtUtc = DateTime.UtcNow;
        return IssueHistoryEntry.Create(Id, IssueAction.Unassigned, performedBy, $"Unassigned from {previous}");
    }

    /// <summary>
    /// Applies optional contextual data from the analysis.
    /// </summary>
    public void ApplyContext(IssueContextData? context)
    {
        if (context is null) return;

        EmailCTS = context.EmailCTS;
        EmailSAM = context.EmailSAM;
        TelCTS = context.TelCTS;
        TelSAM = context.TelSAM;
        FSA = context.FSA;
    }
}
