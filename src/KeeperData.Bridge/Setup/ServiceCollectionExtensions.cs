using KeeperData.Application.Setup;
using KeeperData.Bridge.Authentication;
using KeeperData.Bridge.Config;
using KeeperData.Bridge.Filters;
using KeeperData.Bridge.Utils;
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
using Microsoft.Extensions.DependencyInjection.Extensions;
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
                    var apiAuth = configuration.GetSection(ApiAuthenticationOptions.SectionName).Get<ApiAuthenticationOptions>()
                        ?? new ApiAuthenticationOptions();

                    document.Components ??= new OpenApiComponents();
                    document.Components.SecuritySchemes = new Dictionary<string, IOpenApiSecurityScheme>();
                    document.Security = [];

                    if (apiAuth.ApiKeyEnabled)
                    {
                        // Must match ApiKeyAuthenticationHandler, which reads the Authorization header
                        // and requires the "ApiKey" scheme prefix.
                        document.Components.SecuritySchemes[ApiKeyAuthenticationSchemeOptions.DefaultScheme] = new OpenApiSecurityScheme
                        {
                            Description = "API key authentication, for example "
                                + $"`Authorization: {ApiKeyAuthenticationSchemeOptions.DefaultScheme} <your-key>`.",
                            Type = SecuritySchemeType.Http,
                            Scheme = ApiKeyAuthenticationSchemeOptions.DefaultScheme
                        };

                        document.Security.Add(new OpenApiSecurityRequirement
                        {
                            [new OpenApiSecuritySchemeReference(ApiKeyAuthenticationSchemeOptions.DefaultScheme, document)] = []
                        });
                    }

                    if (apiAuth.Sts.Enabled)
                    {
                        document.Components.SecuritySchemes[StsAuthenticationSchemeOptions.DefaultScheme] = new OpenApiSecurityScheme
                        {
                            Description = "AWS STS presigned GetCallerIdentity authentication, for example "
                                + $"`Authorization: {StsAuthenticationSchemeOptions.DefaultScheme} <base64url-presigned-url>`.",
                            Type = SecuritySchemeType.Http,
                            Scheme = StsAuthenticationSchemeOptions.DefaultScheme
                        };

                        document.Security.Add(new OpenApiSecurityRequirement
                        {
                            [new OpenApiSecuritySchemeReference(StsAuthenticationSchemeOptions.DefaultScheme, document)] = []
                        });
                    }
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
            services.AddOptions<FeatureFlags>().BindConfiguration(FeatureFlags.SectionName);
        }

        private static void ConfigureAuthentication(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddOptions<ApiAuthenticationOptions>().BindConfiguration(ApiAuthenticationOptions.SectionName);

            // UrlEncoder is required for authentication handlers
            services.AddSingleton<UrlEncoder>(_ => UrlEncoder.Default);

            services.TryAddTransient<ProxyHttpMessageHandler>();
            services.AddHttpClient(StsIdentityVerifier.HttpClientName)
                .ConfigureHttpClient((sp, client) =>
                {
                    var apiAuth = sp.GetRequiredService<IConfiguration>()
                        .GetSection(ApiAuthenticationOptions.SectionName)
                        .Get<ApiAuthenticationOptions>() ?? new ApiAuthenticationOptions();
                    client.Timeout = TimeSpan.FromSeconds(Math.Max(1, apiAuth.Sts.VerifyTimeoutSeconds));
                })
                .ConfigurePrimaryHttpMessageHandler<ProxyHttpMessageHandler>();

            services.TryAddSingleton<IStsIdentityVerifier, StsIdentityVerifier>();

            services.AddAuthentication()
                .AddScheme<AuthenticationSchemeOptions, NoAuthenticationHandler>("NoAuth", _ => { })
                .AddScheme<ApiKeyAuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(
                    ApiKeyAuthenticationSchemeOptions.DefaultScheme,
                    _ => { })
                .AddScheme<StsAuthenticationSchemeOptions, StsAuthenticationHandler>(
                    StsAuthenticationSchemeOptions.DefaultScheme,
                    _ => { });

            services.AddOptions<AuthenticationOptions>()
                .Configure<IConfiguration>((options, currentConfig) =>
                {
                    var featureFlags = currentConfig.GetSection(FeatureFlags.SectionName).Get<FeatureFlags>() ?? new FeatureFlags();
                    if (!featureFlags.AuthenticationEnabled)
                    {
                        options.DefaultAuthenticateScheme = "NoAuth";
                        options.DefaultChallengeScheme = "NoAuth";
                        return;
                    }

                    var apiAuth = currentConfig.GetSection(ApiAuthenticationOptions.SectionName).Get<ApiAuthenticationOptions>()
                        ?? new ApiAuthenticationOptions();
                    var defaultScheme = GetEnabledAuthenticationSchemes(apiAuth)[0];
                    options.DefaultAuthenticateScheme = defaultScheme;
                    options.DefaultChallengeScheme = defaultScheme;
                });
        }

        private static void ConfigureAuthorization(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddAuthorization();

            services.AddOptions<AuthorizationOptions>()
                .Configure<IConfiguration>((options, currentConfig) =>
                {
                    options.AddPolicy("AllowAnonymous", policy =>
                    {
                        policy.RequireAssertion(_ => true);
                    });

                    var featureFlags = currentConfig.GetSection(FeatureFlags.SectionName).Get<FeatureFlags>() ?? new FeatureFlags();
                    if (!featureFlags.AuthenticationEnabled)
                    {
                        options.DefaultPolicy = new AuthorizationPolicyBuilder("NoAuth")
                            .RequireAssertion(_ => true)
                            .Build();
                        return;
                    }

                    var apiAuth = currentConfig.GetSection(ApiAuthenticationOptions.SectionName).Get<ApiAuthenticationOptions>()
                        ?? new ApiAuthenticationOptions();
                    var enabledSchemes = GetEnabledAuthenticationSchemes(apiAuth);

                    options.DefaultPolicy = new AuthorizationPolicyBuilder()
                        .RequireAuthenticatedUser()
                        .AddAuthenticationSchemes(enabledSchemes)
                        .Build();
                });

            services.AddControllers(options =>
            {
                options.Filters.Add(new AuthorizeFilter());
                options.Filters.Add<OperationCancelledExceptionFilter>();
            })
            .AddJsonOptions(ConfigureJsonOptions);
        }

        private static string[] GetEnabledAuthenticationSchemes(ApiAuthenticationOptions options)
        {
            var schemes = new List<string>(2);
            if (options.ApiKeyEnabled)
            {
                schemes.Add(ApiKeyAuthenticationSchemeOptions.DefaultScheme);
            }

            if (options.Sts.Enabled)
            {
                schemes.Add(StsAuthenticationSchemeOptions.DefaultScheme);
            }

            if (schemes.Count == 0)
            {
                schemes.Add(ApiKeyAuthenticationSchemeOptions.DefaultScheme);
            }

            return [.. schemes];
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