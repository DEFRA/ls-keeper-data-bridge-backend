namespace KeeperData.Application.Queries.KeyRotations;

/// <summary>
/// Lists successful key rotations, most recent first.
/// </summary>
public record GetKeyRotationsQuery(int Page = 1, int PageSize = 10) : IQuery<KeyRotationListResult>;
