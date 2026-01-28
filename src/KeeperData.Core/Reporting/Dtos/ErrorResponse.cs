using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents an error response from an API operation.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record ErrorResponse
{
    /// <summary>
    /// Gets the error message describing what went wrong.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// Gets the UTC timestamp of the error response.
    /// </summary>
    public DateTime Timestamp { get; init; }

    /// <summary>
    /// Gets the import ID if the error is related to a specific import, or null otherwise.
    /// </summary>
    public Guid? ImportId { get; init; }
}