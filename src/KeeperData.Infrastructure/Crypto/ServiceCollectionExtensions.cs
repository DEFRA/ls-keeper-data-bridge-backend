using KeeperData.Core.Crypto;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Crypto;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddCrypto(this IServiceCollection services, IConfiguration configuration)
    {
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IPasswordSaltService, PasswordSaltService>();
        services.TryAddSingleton<IAesCryptoTransform, AesCryptoTransform>();
    }

}