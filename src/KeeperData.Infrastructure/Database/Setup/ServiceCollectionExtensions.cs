using KeeperData.Core.Database;
using KeeperData.Core.Database.Impl;
using KeeperData.Core.Locking;
using KeeperData.Core.Repositories;
using KeeperData.Core.Transactions;
using KeeperData.Infrastructure.Behaviors;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Database.Factories;
using KeeperData.Infrastructure.Database.Factories.Implementations;
using KeeperData.Infrastructure.Database.Repositories;
using KeeperData.Infrastructure.Database.Transactions;
using KeeperData.Infrastructure.Locking;
using MediatR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using MongoDB.Driver.Authentication.AWS;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Database.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    private static bool s_mongoSerializersRegistered;

    public static void AddDatabaseDependencies(this IServiceCollection services, IConfiguration configuration)
    {
        RegisterMongoDbGlobals();

        var mongoConfig = configuration.GetSection("Mongo").Get<MongoConfig>()!;
        services.Configure<MongoConfig>(configuration.GetSection("Mongo"));

        // Register IDatabaseConfig which wraps MongoConfig
        services.AddSingleton<IOptions<IDatabaseConfig>>(sp =>
        {
            var mongoConfigOptions = sp.GetRequiredService<IOptions<MongoConfig>>();
            return Options.Create<IDatabaseConfig>(mongoConfigOptions.Value);
        });

        services.AddSingleton<IMongoDbClientFactory, MongoDbClientFactory>();
        services.AddScoped<IMongoSessionFactory, MongoSessionFactory>();

        services.AddScoped(sp => sp.GetRequiredService<IMongoSessionFactory>().GetSession());
        services.AddSingleton(sp => sp.GetRequiredService<IMongoDbClientFactory>().CreateClient());
        services.AddScoped(typeof(IGenericRepository<>), typeof(GenericRepository<>));

        services.AddScoped<IUnitOfWork, MongoUnitOfWork>();
        services.AddScoped(sp => (ITransactionManager)sp.GetRequiredService<IUnitOfWork>());

        services.AddScoped<ICollectionManagementService, CollectionManagementService>();

        services.AddTransient(typeof(IPipelineBehavior<,>), typeof(ValidationBehavior<,>));
        services.AddTransient(typeof(IPipelineBehavior<,>), typeof(UnitOfWorkTransactionBehavior<,>));

        services.AddSingleton<IDistributedLock, MongoDistributedLock>();

        if (mongoConfig.HealthcheckEnabled)
        {
            services.AddHealthChecks()
                .AddCheck<MongoDbHealthCheck>("mongodb", tags: ["db", "mongo"]);
        }
    }

    private static void RegisterMongoDbGlobals()
    {
        if (!s_mongoSerializersRegistered)
        {
            lock (typeof(ServiceCollectionExtensions))
            {
                if (!s_mongoSerializersRegistered)
                {
                    BsonSerializer.RegisterSerializer(typeof(Guid), new GuidSerializer(GuidRepresentation.Standard));
                    ConventionRegistry.Register("CamelCase", new ConventionPack { new CamelCaseElementNameConvention() }, _ => true);

                    // Deployed environments authenticate to DocumentDB with authMechanism=MONGODB-AWS, which
                    // driver 3.x only supports once this opt-in runs. The registry throws on a second call.
                    MongoClientSettings.Extensions.AddAWSAuthentication();

                    s_mongoSerializersRegistered = true;
                }
            }
        }
    }
}