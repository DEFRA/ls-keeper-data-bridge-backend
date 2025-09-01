# See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# This stage is used when running from VS in fast mode (Default for Debug configuration)
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080


# This stage is used to build the service project
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["src/KeeperData.Bridge/KeeperData.Bridge.csproj", "src/KeeperData.Bridge/"]
COPY ["src/KeeperData.Core/KeeperData.Core.csproj", "src/KeeperData.Core/"]
COPY ["src/KeeperData.Infrastructure/KeeperData.Infrastructure.csproj", "src/KeeperData.Infrastructure/"]
COPY ["src/KeeperData.Common/KeeperData.Common.csproj", "src/KeeperData.Common/"]
COPY ["src/KeeperData.Data/KeeperData.Data.csproj", "src/KeeperData.Data/"]
RUN dotnet restore "./src/KeeperData.Bridge/KeeperData.Bridge.csproj"
COPY . .
WORKDIR "/src/src/KeeperData.Bridge"
RUN dotnet build "./KeeperData.Bridge.csproj" -c $BUILD_CONFIGURATION -o /app/build

# This stage is used to publish the service project to be copied to the final stage
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./KeeperData.Bridge.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# This stage is used in production or when running from VS in regular mode (Default when not using the Debug configuration)
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "KeeperData.Bridge.dll"]