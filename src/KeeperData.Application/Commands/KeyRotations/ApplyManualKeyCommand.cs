namespace KeeperData.Application.Commands.KeyRotations;

/// <summary>
/// Validates and activates manually supplied external storage credentials.
/// The secret values are write-only: they are never echoed in responses or logs.
/// </summary>
public record ApplyManualKeyCommand(string AccessKeyId, string SecretAccessKey) : ICommand<KeyRotationActionResponse>;
