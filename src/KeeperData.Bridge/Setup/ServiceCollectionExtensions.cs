using KeeperData.Application.Setup;
using KeeperData.Bridge.Authentication;
using KeeperData.Bridge.Config;
using KeeperData.Bridge.Filters;
using KeeperData.Bridge.Worker.Setup;
using KeeperData.Core.ETL.Setup;
using KeeperData.Core.EtlPipeline.Setup;
using KeeperData.Core.Querying.Setup;
using KeeperData.Core.Telemetry;
using KeeperData.Infrastructure.Benchmarking.Setup;
using KeeperData.Infrastructure.Config;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.Database.Setup;
using KeeperData.Infrastructure.ETL.Setup;
using KeeperData.Infrastructure.EtlPipeline.Setup;
using KeeperData.Infrastructure.Extensions;
using KeeperData.Infrastructure.Json;
using KeeperData.Infrastructure.Messaging.Setup;
using KeeperData.Infrastructure.Setup;
using KeeperData.Infrastructure.Storage.Setup;
using KeeperData.Infrastructure.Telemetry;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.Authorization;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.OpenApi;
using System.Diagnostics.CodeAnalysis;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace KeeperData.Bridge.Setup
{
    [ExcludeFromCodeCoverage(Justification = "DI configuration code - tested through integration tests.")]
    public static class ServiceCollectionExtensions
    {
        public static void ConfigureOpenApiDocumentGeneration(this IServiceCollection services)
        {
            services.AddControllers().AddJsonOptions(ConfigureJsonOptions);
            services.ConfigureOpenApiDocument();
        }

        /// <summary>The OpenAPI schema service reads the HTTP JSON options rather than the MVC ones, so the
        /// serializer contract has to be applied there for the generated schemas to match the API's JSON.</summary>
        private static void ConfigureOpenApiDocument(this IServiceCollection services)
        {
            services.ConfigureHttpJsonOptions(options => ConfigureJsonSerializerOptions(options.SerializerOptions));

            services.AddOpenApi("v1", options => options.AddDocumentTransformer((document, context, _) =>
            {
                document.Info = new OpenApiInfo
                {
                    Title = "KeeperData Bridge API",
                    Version = "v1",
                    Description = "API for managing data imports, querying MongoDB collections, and managing external catalogue files. "
                        + "Calls routed through a CDP protected API gateway also need the platform's separate `x-api-key` header.",
                    Contact = new OpenApiContact
                    {
                        Name = "DEFRA",
                        Url = new Uri("https://github.com/DEFRA/ls-keeper-data-bridge-backend")
                    }
                };

                var configuration = context.ApplicationServices.GetRequiredService<IConfiguration>();
                var featureFlags = configuration.GetSection(FeatureFlags.SectionName).Get<FeatureFlags>() ?? new FeatureFlags();

                if (featureFlags.AuthenticationEnabled)
                {
                    document.Components ??= new OpenApiComponents();
                    document.Components.SecuritySchemes = new Dictionary<string, IOpenApiSecurityScheme>
                    {
                        // Must match ApiKeyAuthenticationHandler, which reads the Authorization header
                        // and requires the "ApiKey" scheme prefix.
                        ["ApiKey"] = new OpenApiSecurityScheme
                        {
                            Description = "API key authentication, for example "
                                + $"`Authorization: {ApiKeyAuthenticationSchemeOptions.DefaultScheme} <your-key>`.",
                            Type = SecuritySchemeType.Http,
                            Scheme = ApiKeyAuthenticationSchemeOptions.DefaultScheme
                        }
                    };

                    document.Security =
                    [
                        new OpenApiSecurityRequirement
                        {
                            [new OpenApiSecuritySchemeReference("ApiKey", document)] = []
                        }
                    ];
                }

                return Task.CompletedTask;
            }));
        }

        public static void ConfigureApi(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddDefaultAWSOptions(configuration.GetAWSOptions());
            services.Configure<AwsConfig>(configuration.GetSection(AwsConfig.SectionName));

            services.ConfigureHealthChecks();

            services.ConfigureFeatureFlags(configuration);

            services.ConfigureAuthentication(configuration);

            services.ConfigureAuthorization(configuration);

            services.ConfigureOpenApiDocument();

            services.AddApplicationLayer();

            services.AddDatabaseDependencies(configuration);

            services.AddMessagingDependencies(configuration);

            services.AddStorageDependencies(configuration);

            services.AddEtlPipelineStorageProvider(configuration);

            services.AddEtlDependencies(configuration);

            services.AddEtlServices();

            services.AddEtlImportStatus();

            services.AddCrypto(configuration);

            services.AddBackgroundJobDependencies(configuration);

            services.AddKeeperDataMetrics();


            // Configure OpenTelemetry for metrics
            services.AddOpenTelemetry()
                .WithMetrics(metrics =>
                {
                    metrics.AddMeter(MetricNames.MeterName);
                });

            services.AddMongoQueryService();

            services.AddCleanseReportServices(configuration);

            services.AddBenchmarkServices();
        }

        private static void ConfigureHealthChecks(this IServiceCollection services)
        {
            services.AddHealthChecks()
                .AddCheck<QuartzJobsHealthCheck>("quartz_jobs", tags: ["quartz", "jobs"]);
            services.AddSingleton<IHealthCheckPublisher, HealthCheckMetricsPublisher>();
        }

        private static void ConfigureFeatureFlags(this IServiceCollection services, IConfiguration configuration)
        {
            services.Configure<FeatureFlags>(configuration.GetSection(FeatureFlags.SectionName));
        }

        private static void ConfigureAuthentication(this IServiceCollection services, IConfiguration configuration)
        {
            // Read feature flag configuration
            var featureFlags = configuration.GetSection(FeatureFlags.SectionName).Get<FeatureFlags>() ?? new FeatureFlags();

            if (!featureFlags.AuthenticationEnabled)
            {
                // Authentication is disabled - configure a no-op authentication scheme
                services.AddAuthentication(options =>
                {
                    options.DefaultAuthenticateScheme = "NoAuth";
                    options.DefaultChallengeScheme = "NoAuth";
                })
                .AddScheme<AuthenticationSchemeOptions, NoAuthenticationHandler>("NoAuth", options => { });

                return;
            }

            // UrlEncoder is required for authentication handlers
            services.AddSingleton<UrlEncoder>(_ => UrlEncoder.Default);

            services.AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = ApiKeyAuthenticationSchemeOptions.DefaultScheme;
                options.DefaultChallengeScheme = ApiKeyAuthenticationSchemeOptions.DefaultScheme;
            })
            .AddScheme<ApiKeyAuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(
                ApiKeyAuthenticationSchemeOptions.DefaultScheme,
                options => { });
        }

        private static void ConfigureAuthorization(this IServiceCollection services, IConfiguration configuration)
        {
            // Read feature flag configuration
            var featureFlags = configuration.GetSection(FeatureFlags.SectionName).Get<FeatureFlags>() ?? new FeatureFlags();

            if (!featureFlags.AuthenticationEnabled)
            {
                // Authentication is disabled - configure authorization to allow all requests
                services.AddAuthorization(options =>
                {
                    options.DefaultPolicy = new AuthorizationPolicyBuilder()
                        .RequireAssertion(_ => true) // Allow all requests
                        .Build();
                });

                services.AddControllers(options =>
                    {
                        options.Filters.Add<OperationCancelledExceptionFilter>();
                    })
                    .AddJsonOptions(ConfigureJsonOptions);

                return;
            }

            // Authentication is enabled - configure standard authorization
            services.AddAuthorization(options =>
            {
                // Create a default policy that requires API key authentication
                options.DefaultPolicy = new AuthorizationPolicyBuilder()
                    .RequireAuthenticatedUser()
                    .AddAuthenticationSchemes(ApiKeyAuthenticationSchemeOptions.DefaultScheme)
                    .Build();

                // Create a policy for endpoints that should allow anonymous access
                options.AddPolicy("AllowAnonymous", policy =>
                {
                    policy.RequireAssertion(_ => true); // Always allow
                });
            });

            // Apply authorization globally to all controllers
            services.AddControllers(options =>
            {
                var policy = new AuthorizationPolicyBuilder()
                    .RequireAuthenticatedUser()
                    .AddAuthenticationSchemes(ApiKeyAuthenticationSchemeOptions.DefaultScheme)
                    .Build();
                options.Filters.Add(new AuthorizeFilter(policy));
                options.Filters.Add<OperationCancelledExceptionFilter>();
            })
            .AddJsonOptions(ConfigureJsonOptions);
        }

        /// <summary>Shared so the generated OpenAPI schemas describe the JSON the API actually emits.</summary>
        private static void ConfigureJsonOptions(Microsoft.AspNetCore.Mvc.JsonOptions options)
            => ConfigureJsonSerializerOptions(options.JsonSerializerOptions);

        private static void ConfigureJsonSerializerOptions(JsonSerializerOptions options)
        {
            options.Converters.Add(new JsonStringEnumConverter());
            options.Converters.Add(new BsonDocumentJsonConverter());
        }
    }
}