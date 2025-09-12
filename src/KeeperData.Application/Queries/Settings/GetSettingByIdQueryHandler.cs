using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Repositories;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Application.Queries.Settings;

/// <summary>
/// Example usage only. Delete in future changes.
/// </summary>
/// <param name="repository"></param>
[ExcludeFromCodeCoverage]
public class GetSettingByIdQueryHandler(IGenericRepository<Setting> repository) : IQueryHandler<GetSettingByIdQuery, Setting>
{
    private readonly IGenericRepository<Setting> _repository = repository;

    public async Task<Setting> Handle(GetSettingByIdQuery request, CancellationToken cancellationToken)
    {
        var result = await _repository.GetByIdAsync(request.Id, cancellationToken)
            ?? throw new KeyNotFoundException($"Record with Id {request.Id} not found.");

        return result;
    }
}