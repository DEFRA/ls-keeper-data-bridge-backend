using KeeperData.Core.Storage.KeyRotation;

namespace KeeperData.Application.Commands.KeyRotations;

public class ApplyManualKeyCommandHandler(IKeyRotationService keyRotationService) : ICommandHandler<ApplyManualKeyCommand, KeyRotationActionResponse>
{
    public async Task<KeyRotationActionResponse> Handle(ApplyManualKeyCommand request, CancellationToken cancellationToken)
    {
        var result = await keyRotationService.ApplyManualAsync(request.AccessKeyId, request.SecretAccessKey, cancellationToken);
        return KeyRotationActionResponse.FromResult(result);
    }
}
