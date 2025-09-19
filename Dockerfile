# Base dotnet image
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
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
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
ENV BUILD_CONFIGURATION=${BUILD_CONFIGURATION}
WORKDIR /src

COPY ["src/KeeperData.Bridge/KeeperData.Bridge.csproj", "KeeperData.Bridge/"]
COPY ["src/KeeperData.Infrastructure/KeeperData.Infrastructure.csproj", "KeeperData.Infrastructure/"]
COPY ["src/KeeperData.Application/KeeperData.Application.csproj", "KeeperData.Application/"]
COPY ["src/KeeperData.Core/KeeperData.Core.csproj", "KeeperData.Core/"]

RUN dotnet restore "KeeperData.Bridge/KeeperData.Bridge.csproj" -r linux-x64 -v n
RUN dotnet restore "KeeperData.Infrastructure/KeeperData.Infrastructure.csproj" -r linux-x64 -v n
RUN dotnet restore "KeeperData.Application/KeeperData.Application.csproj" -r linux-x64 -v n
RUN dotnet restore "KeeperData.Core/KeeperData.Core.csproj" -r linux-x64 -v n

COPY ["src/", "."]

# Run all tests except those with "Integration" in the path
RUN find ./tests -name '*.csproj' | grep -v Integration | while read testproj; do \
    dotnet test "$testproj" --configuration ${BUILD_CONFIGURATION} --no-restore --verbosity normal; \
done

FROM build AS publish
WORKDIR "/src/KeeperData.Bridge"
RUN dotnet publish "KeeperData.Bridge.csproj" -v n -c ${BUILD_CONFIGURATION} -o /app/publish -r linux-x64 --no-restore /p:UseAppHost=false

ENV ASPNETCORE_FORWARDEDHEADERS_ENABLED=true

# Final production image
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
EXPOSE 8085
ENTRYPOINT ["dotnet", "KeeperData.Bridge.dll"]
