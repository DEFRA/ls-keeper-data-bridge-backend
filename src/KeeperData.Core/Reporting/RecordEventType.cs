namespace KeeperData.Core.Reporting;

/// <summary>
/// Represents the type of event recorded in a record's lineage.
/// </summary>
public enum RecordEventType
{
    /// <summary>
    /// The record was newly created.
    /// </summary>
    Created,

    /// <summary>
    /// An existing record was updated.
    /// </summary>
    Updated,

    /// <summary>
    /// The record was deleted.
    /// </summary>
    Deleted,

    /// <summary>
    /// A previously deleted record was restored.
    /// </summary>
    Undeleted
}