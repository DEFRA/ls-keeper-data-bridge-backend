using KeeperData.Core.Storage.KeyRotation;

namespace KeeperData.Application.Queries.KeyRotations;

public class GetKeyRotationsQueryHandler(IKeyRotationRepository repository) : IQueryHandler<GetKeyRotationsQuery, KeyRotationListResult>
{
    public async Task<KeyRotationListResult> Handle(GetKeyRotationsQuery request, CancellationToken cancellationToken)
    {
        var page = await repository.GetSuccessfulPageAsync(request.Page, request.PageSize, cancellationToken);

        return new KeyRotationListResult
        {
            Items = page.Items.Select(KeyRotationListItem.FromRecord).ToList(),
            Page = request.Page,
            PageSize = request.PageSize,
            TotalCount = page.TotalCount
        };
    }
}
