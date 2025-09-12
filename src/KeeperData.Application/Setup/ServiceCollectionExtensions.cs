using FluentValidation;
using Microsoft.Extensions.DependencyInjection;

namespace KeeperData.Application.Setup;

public static class ServiceCollectionExtensions
{
    public static void AddApplicationLayer(this IServiceCollection services)
    {
        services.AddMediatR(cfg =>
        {
            cfg.RegisterServicesFromAssembly(typeof(IRequestExecutor).Assembly);
        });

        services.AddScoped<IRequestExecutor, RequestExecutor>();
        services.AddValidatorsFromAssemblyContaining<IRequestExecutor>();
    }
}