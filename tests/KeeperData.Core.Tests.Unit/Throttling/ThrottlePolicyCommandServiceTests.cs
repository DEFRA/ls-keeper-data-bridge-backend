using FluentAssertions;
using KeeperData.Core.Exceptions;
using KeeperData.Core.Throttling;
using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Commands;
using KeeperData.Core.Throttling.Impl;
using KeeperData.Core.Throttling.Models;
using Moq;

namespace KeeperData.Core.Tests.Unit.Throttling;

public class ThrottlePolicyCommandServiceTests
{
    private readonly Mock<IThrottlePolicyRepository> _repoMock = new();
    private readonly ThrottlePolicyCommandService _sut;

    public ThrottlePolicyCommandServiceTests()
    {
        _sut = new ThrottlePolicyCommandService(_repoMock.Object);
    }

    [Fact]
    public async Task CreateAsync_WithValidCommand_ShouldPersist()
    {
        var cmd = new CreateThrottlePolicyCommand
        {
            Name = "My Policy",
            Settings = new ThrottlePolicySettings()
        };

        var result = await _sut.CreateAsync(cmd);

        result.Slug.Should().Be("my-policy");
        result.Name.Should().Be("My Policy");
        _repoMock.Verify(r => r.UpsertAsync(It.IsAny<ThrottlePolicy>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateAsync_WithNormalName_ShouldThrow()
    {
        var cmd = new CreateThrottlePolicyCommand { Name = "Normal", Settings = new() };

        var act = () => _sut.CreateAsync(cmd);

        await act.Should().ThrowAsync<DomainException>().WithMessage("*reserved*");
    }

    [Fact]
    public async Task CreateAsync_WithDuplicateSlug_ShouldThrow()
    {
        _repoMock.Setup(r => r.GetBySlugAsync("my-policy", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ThrottlePolicy { Slug = "my-policy", Name = "My Policy", Settings = new() });

        var cmd = new CreateThrottlePolicyCommand { Name = "My Policy", Settings = new() };

        var act = () => _sut.CreateAsync(cmd);

        await act.Should().ThrowAsync<DomainException>().WithMessage("*already exists*");
    }

    [Fact]
    public async Task UpdateAsync_NormalSlug_ShouldThrow()
    {
        var cmd = new UpdateThrottlePolicyCommand { Settings = new() };

        var act = () => _sut.UpdateAsync("normal", cmd);

        await act.Should().ThrowAsync<DomainException>().WithMessage("*Normal*");
    }

    [Fact]
    public async Task UpdateAsync_NotFound_ShouldThrow()
    {
        var cmd = new UpdateThrottlePolicyCommand { Settings = new() };

        var act = () => _sut.UpdateAsync("missing", cmd);

        await act.Should().ThrowAsync<NotFoundException>();
    }

    [Fact]
    public async Task DeleteAsync_NormalSlug_ShouldThrow()
    {
        var act = () => _sut.DeleteAsync("normal");

        await act.Should().ThrowAsync<DomainException>();
    }

    [Fact]
    public async Task DeleteAsync_ActivePolicy_ShouldThrow()
    {
        _repoMock.Setup(r => r.GetBySlugAsync("active", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ThrottlePolicy { Slug = "active", Name = "Active", IsActive = true, Settings = new() });

        var act = () => _sut.DeleteAsync("active");

        await act.Should().ThrowAsync<DomainException>().WithMessage("*active*");
    }

    [Fact]
    public async Task ActivateAsync_NormalSlug_ShouldThrow()
    {
        var act = () => _sut.ActivateAsync("normal");

        await act.Should().ThrowAsync<DomainException>();
    }

    [Fact]
    public async Task ActivateAsync_ValidSlug_ShouldDeactivateAllThenActivate()
    {
        _repoMock.Setup(r => r.GetBySlugAsync("test", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ThrottlePolicy { Slug = "test", Name = "Test", Settings = new() });

        var result = await _sut.ActivateAsync("test");

        result.IsActive.Should().BeTrue();
        _repoMock.Verify(r => r.DeactivateAllAsync(It.IsAny<CancellationToken>()), Times.Once);
        _repoMock.Verify(r => r.UpsertAsync(It.Is<ThrottlePolicy>(p => p.IsActive), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task DeactivateAllAsync_ShouldCallRepo()
    {
        await _sut.DeactivateAllAsync();

        _repoMock.Verify(r => r.DeactivateAllAsync(It.IsAny<CancellationToken>()), Times.Once);
    }
}
