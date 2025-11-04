using KeeperData.Core.Reporting.Domain;
using KeeperData.Core.Reporting.Dtos;

namespace KeeperData.Core.Reporting.Services;

/// <summary>
/// Service responsible for mapping between domain models and DTOs.
/// Follows Single Responsibility Principle - only handles mapping logic.
/// Pure functions with no side effects.
/// </summary>
public interface ILineageMapper
{
    /// <summary>
    /// Maps domain event to LineageEventDocument for persistence.
    /// </summary>
    LineageEventDocument MapToEventDocument(
        RecordLineageEvent lineageEvent,
        string eventId,
        string lineageDocumentId);

    /// <summary>
    /// Maps domain models to RecordLifecycle DTO.
    /// </summary>
    RecordLifecycle MapToRecordLifecycle(
        RecordLineageDocument parentDoc,
        IReadOnlyList<LineageEventDocument> events);

    /// <summary>
    /// Maps event document to RecordLineageEvent DTO.
    /// </summary>
    RecordLineageEvent MapToLineageEvent(LineageEventDocument eventDoc, string recordId, string collectionName);
}

/// <summary>
/// Default implementation of lineage mapping.
/// </summary>
public class LineageMapper : ILineageMapper
{
    public LineageEventDocument MapToEventDocument(
        RecordLineageEvent lineageEvent,
        string eventId,
        string lineageDocumentId)
    {
        if (lineageEvent == null) throw new ArgumentNullException(nameof(lineageEvent));
        if (string.IsNullOrWhiteSpace(eventId)) throw new ArgumentException("Event ID cannot be null or whitespace.", nameof(eventId));
        if (string.IsNullOrWhiteSpace(lineageDocumentId)) throw new ArgumentException("Lineage document ID cannot be null or whitespace.", nameof(lineageDocumentId));

        return new LineageEventDocument
        {
            Id = eventId,
            LineageDocumentId = lineageDocumentId,
            RecordId = lineageEvent.RecordId,
            CollectionName = lineageEvent.CollectionName,
            EventType = lineageEvent.EventType.ToString(),
            ImportId = lineageEvent.ImportId,
            FileKey = lineageEvent.FileKey,
            EventDateUtc = lineageEvent.EventDateUtc,
            ChangeType = lineageEvent.ChangeType,
            PreviousValues = lineageEvent.PreviousValues,
            NewValues = lineageEvent.NewValues
        };
    }

    public RecordLifecycle MapToRecordLifecycle(
        RecordLineageDocument parentDoc,
        IReadOnlyList<LineageEventDocument> events)
    {
        if (parentDoc == null) throw new ArgumentNullException(nameof(parentDoc));
        if (events == null) throw new ArgumentNullException(nameof(events));

        return new RecordLifecycle
        {
            RecordId = parentDoc.RecordId,
            CollectionName = parentDoc.CollectionName,
            CurrentStatus = parentDoc.CurrentStatus,
            CreatedByImport = parentDoc.CreatedByImport,
            LastModifiedByImport = parentDoc.LastModifiedByImport,
            CreatedAtUtc = parentDoc.CreatedAtUtc,
            LastModifiedAtUtc = parentDoc.LastModifiedAtUtc,
            Events = events.Select(e => MapToLineageEvent(e, parentDoc.RecordId, parentDoc.CollectionName)).ToList()
        };
    }

    public RecordLineageEvent MapToLineageEvent(LineageEventDocument eventDoc, string recordId, string collectionName)
    {
        if (eventDoc == null) throw new ArgumentNullException(nameof(eventDoc));

        return new RecordLineageEvent
        {
            RecordId = recordId,
            CollectionName = collectionName,
            EventType = Enum.Parse<RecordEventType>(eventDoc.EventType),
            ImportId = eventDoc.ImportId,
            FileKey = eventDoc.FileKey,
            EventDateUtc = eventDoc.EventDateUtc,
            ChangeType = eventDoc.ChangeType,
            PreviousValues = eventDoc.PreviousValues,
            NewValues = eventDoc.NewValues
        };
    }
}
