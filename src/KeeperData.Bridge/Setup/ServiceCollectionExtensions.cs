using KeeperData.Application.Setup;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Bridge.Worker.Setup;
using KeeperData.Infrastructure.Database.Setup;
using KeeperData.Infrastructure.Messaging.Setup;
using KeeperData.Infrastructure.Storage.Setup;
using KeeperData.Bridge.Config;
using KeeperData.Bridge.Authentication;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.Authorization;
using System.Text.Encodings.Web;
using System.Text.Json.Serialization;
using KeeperData.Core.ETL.Setup;
using KeeperData.Core.Querying.Setup;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Models;
using System.Reflection;

namespace KeeperData.Bridge.Setup
{
    public static class ServiceCollectionExtensions
    {
        public static void ConfigureApi(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddDefaultAWSOptions(configuration.GetAWSOptions());

            services.ConfigureHealthChecks();

            services.ConfigureFeatureFlags(configuration);

            services.ConfigureAuthentication(configuration);

            services.ConfigureAuthorization(configuration);

            services.ConfigureSwagger(configuration);

            services.AddApplicationLayer();

            services.AddDatabaseDependencies(configuration);

            services.AddMessagingDependencies(configuration);

            services.AddStorageDependencies(configuration);

            services.AddEtlDependencies();

            services.AddCrypto(configuration);

            services.AddBackgroundJobDependencies(configuration);

            services.AddMongoQueryService();
        }

        private static void ConfigureHealthChecks(this IServiceCollection services)
        {
            services.AddHealthChecks();
        }

        private static void ConfigureFeatureFlags(this IServiceCollection services, IConfiguration configuration)
        {
            services.Configure<FeatureFlags>(configuration.GetSection(FeatureFlags.SectionName));
        }

        private static void ConfigureSwagger(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddEndpointsApiExplorer();
            services.AddSwaggerGen(options =>
            {
                options.SwaggerDoc("v1", new OpenApiInfo
                {
                    Title = "KeeperData Bridge API",
                    Version = "v1",
                    Description = "API for managing data imports, querying MongoDB collections, and managing external catalogue files",
                    Contact = new OpenApiContact
                    {
                        Name = "DEFRA",
                        Url = new Uri("https://github.com/DEFRA/ls-keeper-data-bridge-backend")
                    }
                });

                // Include XML comments for better documentation
                var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
                var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
                if (File.Exists(xmlPath))
                {
                    options.IncludeXmlComments(xmlPath);
                }

                // Check if authentication is enabled
                var featureFlags = configuration.GetSection(FeatureFlags.SectionName).Get<FeatureFlags>() ?? new FeatureFlags();

                if (featureFlags.AuthenticationEnabled)
                {
                    // Add API Key authentication support in Swagger UI
                    options.AddSecurityDefinition("ApiKey", new OpenApiSecurityScheme
                    {
                        Description = "API Key authentication. Enter your API key in the 'X-API-Key' header.",
                        Name = "X-API-Key",
                        In = ParameterLocation.Header,
                        Type = SecuritySchemeType.ApiKey,
                        Scheme = "ApiKeyScheme"
                    });

                    options.AddSecurityRequirement(new OpenApiSecurityRequirement
                    {
                        {
                            new OpenApiSecurityScheme
                            {
                                Reference = new OpenApiReference
                                {
                                    Type = ReferenceType.SecurityScheme,
                                    Id = "ApiKey"
                                }
                            },
                            Array.Empty<string>()
                        }
                    });
                }
            });
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

                services.AddControllers()
                    .AddJsonOptions(opts =>
                    {
                        var enumConverter = new JsonStringEnumConverter();
                        opts.JsonSerializerOptions.Converters.Add(enumConverter);
                    });

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
            })
            .AddJsonOptions(opts =>
            {
                var enumConverter = new JsonStringEnumConverter();
                opts.JsonSerializerOptions.Converters.Add(enumConverter);
            });
        }
    }
}