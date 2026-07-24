namespace KeeperData.Application.Commands.KeyRotations;

/// <summary>
/// Rolls back to the credentials captured in a previous rotation record,
/// re-validating them against the bucket before activation.
/// </summary>
public record RollbackKeyRotationCommand(string RotationId) : ICommand<KeyRotationActionResponse>;
