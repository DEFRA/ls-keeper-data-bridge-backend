# syntax=docker/dockerfile:1
# Base dotnet image
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

# Add curl to template.
# CDP PLATFORM HEALTHCHECK REQUIREMENT
RUN apt update && \
    apt install curl -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Build stage image
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
ENV BUILD_CONFIGURATION=${BUILD_CONFIGURATION}

# The NuGet token is supplied as a BuildKit secret and is never persisted in an image layer.

WORKDIR /src

COPY ["src/KeeperData.Bridge/KeeperData.Bridge.csproj", "KeeperData.Bridge/"]
COPY ["src/KeeperData.Bridge.Worker/KeeperData.Bridge.Worker.csproj", "KeeperData.Bridge.Worker/"]
COPY ["src/KeeperData.Infrastructure/KeeperData.Infrastructure.csproj", "KeeperData.Infrastructure/"]
COPY ["src/KeeperData.Application/KeeperData.Application.csproj", "KeeperData.Application/"]
COPY ["src/KeeperData.Core/KeeperData.Core.csproj", "KeeperData.Core/"]
COPY ["src/KeeperData.Core.Reports/KeeperData.Core.Reports.csproj", "KeeperData.Core.Reports/"]

COPY ["nuget.config", "."]

RUN --mount=type=secret,id=nuget_auth_token \
    token="$(cat /run/secrets/nuget_auth_token)" \
    && dotnet nuget update source DEFRA --username "github-actions" --password "$token" --store-password-in-clear-text --configfile ./nuget.config \
    && dotnet restore "KeeperData.Bridge/KeeperData.Bridge.csproj" -r linux-x64 -v n \
    && dotnet restore "KeeperData.Bridge.Worker/KeeperData.Bridge.Worker.csproj" -r linux-x64 -v n \
    && dotnet restore "KeeperData.Infrastructure/KeeperData.Infrastructure.csproj" -r linux-x64 -v n \
    && dotnet restore "KeeperData.Application/KeeperData.Application.csproj" -r linux-x64 -v n \
    && dotnet restore "KeeperData.Core/KeeperData.Core.csproj" -r linux-x64 -v n \
    && dotnet restore "KeeperData.Core.Reports/KeeperData.Core.Reports.csproj" -r linux-x64 -v n \
    && rm nuget.config

COPY ["src/", "."]

FROM build AS publish
WORKDIR "/src/KeeperData.Bridge"
RUN dotnet publish "KeeperData.Bridge.csproj" -v n -c "${BUILD_CONFIGURATION}" -o /app/publish -r linux-x64 --no-restore /p:UseAppHost=false

ENV ASPNETCORE_FORWARDEDHEADERS_ENABLED=true

# Final production image
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
EXPOSE 8085
ENTRYPOINT ["dotnet", "KeeperData.Bridge.dll"]
