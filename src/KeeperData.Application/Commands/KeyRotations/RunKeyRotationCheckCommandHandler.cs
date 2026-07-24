using KeeperData.Core.Storage.KeyRotation;
using Microsoft.Extensions.Logging;

namespace KeeperData.Application.Commands.KeyRotations;

public class RunKeyRotationCheckCommandHandler(
    IKeyRotationService keyRotationService,
    TimeProvider timeProvider,
    ILogger<RunKeyRotationCheckCommandHandler> logger) : ICommandHandler<RunKeyRotationCheckCommand, KeyRotationCheckResponse>
{
    private const string LogPrefix = "[KeyRotation]";

    public async Task<KeyRotationCheckResponse> Handle(RunKeyRotationCheckCommand request, CancellationToken cancellationToken)
    {
        logger.LogInformation("{LogPrefix} On-demand key rotation check requested via API", LogPrefix);

        var result = await keyRotationService.CheckAndRotateAsync(cancellationToken);

        logger.LogInformation(
            "{LogPrefix} On-demand key rotation check finished with outcome {Outcome} (bucket {BucketName}, file {FileKey}, hash {FileHash}, key {KeyIdHint}): {Detail}",
            LogPrefix, result.Outcome, result.BucketName, result.FileKey, result.FileHash, result.KeyIdHint, result.Detail);

        return KeyRotationCheckResponse.FromResult(result, timeProvider.GetUtcNow().UtcDateTime);
    }
}
