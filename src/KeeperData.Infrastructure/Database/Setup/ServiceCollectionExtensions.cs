using KeeperData.Core.Repositories;
using KeeperData.Core.Transactions;
using KeeperData.Infrastructure.Behaviors;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Database.Factories;
using KeeperData.Infrastructure.Database.Factories.Implementations;
using KeeperData.Infrastructure.Database.Repositories;
using KeeperData.Infrastructure.Database.Transactions;
using MediatR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.Serializers;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Database.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddDatabaseDependencies(this IServiceCollection services, IConfiguration configuration)
    {
        BsonSerializer.RegisterSerializer(typeof(Guid), new GuidSerializer(GuidRepresentation.Standard));
        ConventionRegistry.Register("CamelCase", new ConventionPack { new CamelCaseElementNameConvention() }, _ => true);

        var mongoConfig = configuration.GetSection("Mongo").Get<MongoConfig>()!;
        services.Configure<MongoConfig>(configuration.GetSection("Mongo"));

        services.AddSingleton<IMongoDbClientFactory, MongoDbClientFactory>();
        services.AddScoped<IMongoSessionFactory, MongoSessionFactory>();

        services.AddScoped(sp => sp.GetRequiredService<IMongoSessionFactory>().GetSession());
        services.AddSingleton(sp => sp.GetRequiredService<IMongoDbClientFactory>().CreateClient());
        services.AddScoped(typeof(IGenericRepository<>), typeof(GenericRepository<>));

        services.AddScoped<IUnitOfWork, MongoUnitOfWork>();
        services.AddScoped(sp => (ITransactionManager)sp.GetRequiredService<IUnitOfWork>());

        services.AddTransient(typeof(IPipelineBehavior<,>), typeof(ValidationBehavior<,>));
        services.AddTransient(typeof(IPipelineBehavior<,>), typeof(UnitOfWorkTransactionBehavior<,>));

        if (mongoConfig.HealthcheckEnabled)
        {
            services.AddHealthChecks()
                .AddCheck<MongoDbHealthCheck>("mongodb", tags: ["db", "mongo"]);
        }
    }
}