using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Repositories;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Application.Commands.Settings;

/// <summary>
/// Example usage only. Delete in future changes.
/// </summary>
/// <param name="repository"></param>
[ExcludeFromCodeCoverage]
public class CreateSettingCommandHandler(IGenericRepository<Setting> repository) : ICommandHandler<CreateSettingCommand, string>
{
    private readonly IGenericRepository<Setting> _repository = repository;

    public async Task<string> Handle(CreateSettingCommand request, CancellationToken cancellationToken)
    {
        var entity = new Setting
        {
            Id = Guid.NewGuid().ToString(),
            Name = request.Name
        };

        await _repository.AddAsync(entity, cancellationToken);

        return entity.Id;
    }
}