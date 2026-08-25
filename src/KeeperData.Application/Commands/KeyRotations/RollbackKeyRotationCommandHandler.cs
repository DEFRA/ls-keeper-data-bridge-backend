using KeeperData.Core.Storage.KeyRotation;

namespace KeeperData.Application.Commands.KeyRotations;

public class RollbackKeyRotationCommandHandler(IKeyRotationService keyRotationService) : ICommandHandler<RollbackKeyRotationCommand, KeyRotationActionResponse>
{
    public async Task<KeyRotationActionResponse> Handle(RollbackKeyRotationCommand request, CancellationToken cancellationToken)
    {
        var result = await keyRotationService.RollbackAsync(request.RotationId, cancellationToken);
        return KeyRotationActionResponse.FromResult(result);
    }
}
