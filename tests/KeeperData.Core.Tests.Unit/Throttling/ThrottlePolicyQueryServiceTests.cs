using FluentAssertions;
using KeeperData.Core.Throttling;
using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Impl;
using KeeperData.Core.Throttling.Models;
using Moq;

namespace KeeperData.Core.Tests.Unit.Throttling;

public class ThrottlePolicyQueryServiceTests
{
    private readonly Mock<IThrottlePolicyRepository> _repoMock = new();
    private readonly ThrottlePolicyProvider _provider = new();
    private readonly ThrottlePolicyQueryService _sut;

    public ThrottlePolicyQueryServiceTests()
    {
        _sut = new ThrottlePolicyQueryService(_repoMock.Object, _provider);
    }

    [Fact]
    public async Task GetAllAsync_ShouldIncludeNormalPolicyFirst()
    {
        _repoMock.Setup(r => r.GetAllAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync([new ThrottlePolicy { Slug = "test", Name = "Test", Settings = new() }]);

        var result = await _sut.GetAllAsync();

        result.Should().HaveCount(2);
        result[0].Slug.Should().Be("normal");
        result[0].IsReadOnly.Should().BeTrue();
        result[1].Slug.Should().Be("test");
    }

    [Fact]
    public async Task GetBySlugAsync_Normal_ShouldReturnNormalPolicy()
    {
        var result = await _sut.GetBySlugAsync("normal");

        result.Should().NotBeNull();
        result!.IsReadOnly.Should().BeTrue();
    }

    [Fact]
    public async Task GetBySlugAsync_NonExistent_ShouldReturnNull()
    {
        _repoMock.Setup(r => r.GetBySlugAsync("missing", It.IsAny<CancellationToken>()))
            .ReturnsAsync((ThrottlePolicy?)null);

        var result = await _sut.GetBySlugAsync("missing");

        result.Should().BeNull();
    }

    [Fact]
    public void GetActive_WhenNoActivePolicy_ShouldReturnNormal()
    {
        var result = _sut.GetActive();

        result.Name.Should().Be(ThrottlePolicyDefaults.NormalName);
        result.IsActive.Should().BeTrue();
    }

    [Fact]
    public void GetActive_WhenPolicyRefreshed_ShouldReturnRefreshedPolicy()
    {
        _provider.Refresh(new ThrottlePolicy
        {
            Slug = "fast",
            Name = "Fast",
            IsActive = true,
            Settings = new() { Ingestion = new() { BatchSize = 999 } }
        });

        var result = _sut.GetActive();

        result.Name.Should().Be("Fast");
        result.Settings.Ingestion.BatchSize.Should().Be(999);
    }
}
