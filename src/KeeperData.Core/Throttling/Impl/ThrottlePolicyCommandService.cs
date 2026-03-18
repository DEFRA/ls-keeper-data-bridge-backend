using KeeperData.Core.Exceptions;
using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Commands;
using KeeperData.Core.Throttling.Models;
using KeeperData.Core.Throttling.Validation;

namespace KeeperData.Core.Throttling.Impl;

public sealed class ThrottlePolicyCommandService(
    IThrottlePolicyRepository repository) : IThrottlePolicyCommandService
{
    public async Task<ThrottlePolicy> CreateAsync(CreateThrottlePolicyCommand command, CancellationToken ct = default)
    {
        var errors = ThrottlePolicyValidator.Validate(command.Name, command.Settings);
        if (errors.Count > 0)
            throw new DomainException(string.Join(" ", errors));

        var slug = ThrottlePolicyDefaults.ToSlug(command.Name);

        if (slug == ThrottlePolicyDefaults.NormalSlug)
            throw new DomainException("Cannot create a policy with the reserved name 'Normal'.");

        var existing = await repository.GetBySlugAsync(slug, ct);
        if (existing is not null)
            throw new DomainException($"A policy with slug '{slug}' already exists.");

        var policy = new ThrottlePolicy
        {
            Slug = slug,
            Name = command.Name,
            IsActive = false,
            IsReadOnly = false,
            Settings = command.Settings,
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow
        };

        await repository.UpsertAsync(policy, ct);
        return policy;
    }

    public async Task<ThrottlePolicy> UpdateAsync(string slug, UpdateThrottlePolicyCommand command, CancellationToken ct = default)
    {
        if (slug == ThrottlePolicyDefaults.NormalSlug)
            throw new DomainException("The 'Normal' policy cannot be modified.");

        var existing = await repository.GetBySlugAsync(slug, ct)
            ?? throw new NotFoundException($"Policy '{slug}' not found.");

        var name = command.Name ?? existing.Name;
        var errors = ThrottlePolicyValidator.Validate(name, command.Settings);
        if (errors.Count > 0)
            throw new DomainException(string.Join(" ", errors));

        var updated = existing with
        {
            Name = name,
            Settings = command.Settings,
            UpdatedAtUtc = DateTime.UtcNow
        };

        await repository.UpsertAsync(updated, ct);
        return updated;
    }

    public async Task DeleteAsync(string slug, CancellationToken ct = default)
    {
        if (slug == ThrottlePolicyDefaults.NormalSlug)
            throw new DomainException("The 'Normal' policy cannot be deleted.");

        var existing = await repository.GetBySlugAsync(slug, ct)
            ?? throw new NotFoundException($"Policy '{slug}' not found.");

        if (existing.IsActive)
            throw new DomainException("Cannot delete the currently active policy. Deactivate it first.");

        await repository.DeleteAsync(slug, ct);
    }

    public async Task<ThrottlePolicy> ActivateAsync(string slug, CancellationToken ct = default)
    {
        if (slug == ThrottlePolicyDefaults.NormalSlug)
            throw new DomainException("The 'Normal' policy cannot be activated. Deactivate all policies to use Normal.");

        var policy = await repository.GetBySlugAsync(slug, ct)
            ?? throw new NotFoundException($"Policy '{slug}' not found.");

        await repository.DeactivateAllAsync(ct);

        var activated = policy with { IsActive = true, UpdatedAtUtc = DateTime.UtcNow };
        await repository.UpsertAsync(activated, ct);

        return activated;
    }

    public async Task DeactivateAllAsync(CancellationToken ct = default)
    {
        await repository.DeactivateAllAsync(ct);
    }
}
