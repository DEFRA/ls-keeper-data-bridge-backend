# LS Keeper Data Bridge Backend

A core delivery C# ASP.NET Core 8 backend service for the Land Services Keeper Data Bridge, providing data integration capabilities with MongoDB, AWS services, and Redis.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Local Development Setup](#local-development-setup)
  - [MongoDB Setup](#mongodb-setup)
  - [Environment Variables](#environment-variables)
- [Running the Application](#running-the-application)
- [Testing](#testing)
- [Development](#development)
  - [Building](#building)
  - [Code Quality](#code-quality)
  - [API Documentation](#api-documentation)
- [Deployment](#deployment)
  - [Docker](#docker)
  - [CDP Environments](#cdp-environments)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [Licence](#licence)

## Overview

This project provides a data bridge service that:
- Integrates with MongoDB for data persistence
- Leverages AWS services (S3, SQS) via LocalStack for local development
- Implements caching with Redis
- Provides REST API endpoints for data import and querying
- Includes comprehensive unit, component, and integration tests

**Technology Stack:**
- .NET 8
- ASP.NET Core
- MongoDB
- Redis
- AWS (LocalStack for local development)
- Docker & Docker Compose

## Prerequisites

- **.NET 8 SDK** - [Download](https://dotnet.microsoft.com/download/dotnet/8.0)
- **Docker & Docker Compose** - [Download](https://www.docker.com/products/docker-desktop)
- **Git** - [Download](https://git-scm.com/)
- **MongoDB CLI tools** (optional) - [mongosh](https://www.mongodb.com/docs/mongodb-shell/install/)

## Project Structure

```
├── src/
│   ├── KeeperData.Bridge/              # Main ASP.NET Core web application
│   ├── KeeperData.Bridge.Worker/       # Background worker service
│   ├── KeeperData.Application/         # Application/use case logic
│   ├── KeeperData.Infrastructure/      # Data access and external integrations
│   ├── KeeperData.Core/                # Core domain models and entities
│   └── KeeperData.Crypto.Tool/         # Encryption utility
├── tests/
│   ├── KeeperData.Bridge.Tests.Unit/
│   ├── KeeperData.Bridge.Tests.Integration/
│   ├── KeeperData.Bridge.Tests.Component/
│   ├── KeeperData.Application.Tests.Unit/
│   ├── KeeperData.Core.Tests.Unit/
│   ├── KeeperData.Infrastructure.Tests.Unit/
│   └── KeeperData.Bridge.PerformanceTests/
├── compose.yml                         # Docker Compose configuration
└── README.md                           # This file
```

## Getting Started

### Local Development Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/DEFRA/ls-keeper-data-bridge-backend.git
   cd ls-keeper-data-bridge-backend
   ```

2. **Restore NuGet packages:**
   ```bash
   dotnet restore
   ```

3. **Start the local development environment:**
   ```bash
   docker compose up --build -d
   ```

   This starts:
   - MongoDB
   - Redis
   - LocalStack (S3, SQS)
   - This service
   - (Optional) Frontend example (commented out by default)

4. **Verify services are running:**
   ```bash
   docker compose ps
   ```

### MongoDB Setup

#### Option 1: Docker (Recommended)

MongoDB is included in the Docker Compose setup:

```bash
docker compose up -d mongodb
```

#### Option 2: Local Installation

- Install [MongoDB Community Server](https://www.mongodb.com/docs/manual/tutorial/#installation)
- Start MongoDB:
  ```bash
  # macOS / Linux
  sudo mongod --dbpath ~/mongodb-cdp
  
  # Windows (run as Administrator)
  mongod --dbpath C:\mongodb-cdp
  ```

#### Option 3: CDP Environments

In CDP environments, MongoDB is pre-configured and credentials are exposed via environment variables.

### Inspect MongoDB

To view databases and collections:

```bash
# Connect to MongoDB shell
mongosh

# Common commands
show databases
use keeper-data
show collections
db.collection_name.find().pretty()
```

You can also use the CDP Terminal to access MongoDB in remote environments.

### Environment Variables

Create a `.env` file or set environment variables for local development:

```bash
# MongoDB
MONGODB_CONNECTION_STRING=mongodb://localhost:27017
MONGODB_DATABASE_NAME=keeper-data

# Redis (optional)
REDIS_CONNECTION_STRING=localhost:6379

# AWS/LocalStack
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_REGION=eu-west-1
LOCALSTACK_ENDPOINT=http://localhost:4566

# API Configuration
ASPNETCORE_ENVIRONMENT=Development
ASPNETCORE_URLS=https://localhost:5001;http://localhost:5000
```

## Running the Application

### Development Mode

```bash
dotnet run --project src/KeeperData.Bridge --launch-profile Development
```

The API will be available at:
- HTTP: `http://localhost:5000`
- HTTPS: `https://localhost:5001`

### Using Docker

```bash
docker compose up -d keeper-data-bridge
```

### View Logs

```bash
# Watch application logs
dotnet run --project src/KeeperData.Bridge

# Or with Docker
docker compose logs -f keeper-data-bridge
```

## Testing

### Run All Tests

```bash
dotnet test
```

### Run Specific Test Project

```bash
dotnet test tests/KeeperData.Bridge.Tests.Unit
dotnet test tests/KeeperData.Bridge.Tests.Integration
dotnet test tests/KeeperData.Bridge.Tests.Component
```

### Test with Coverage

```bash
dotnet test /p:CollectCoverage=true /p:CoverageFormat=opencover
```

**Testing Approach:**
- Tests use a full `WebApplication` instance backed by [Ephemeral MongoDB](https://github.com/asimmon/ephemeral-mongo)
- No mocking - tests read and write from in-memory database
- Ensures tests reflect real application behavior

## Development

### Building

```bash
# Build solution
dotnet build

# Build specific project
dotnet build src/KeeperData.Bridge

# Build with specific configuration
dotnet build -c Release
```

### Code Quality

#### SonarCloud

SonarCloud configuration examples are available in GitHub Action workflows. To set up:

1. Connect your repository to [SonarCloud](https://sonarcloud.io)
2. Add project key to your CI/CD pipeline
3. SonarCloud will analyze code on each push

#### Code Formatting

```bash
# Format all code
dotnet format

# Format specific project
dotnet format src/KeeperData.Bridge
```

### API Documentation

The application provides API endpoints:

- **POST /api/import** - Import data
- **GET /api/query** - Query data
- **GET /api/external-catalogue** - Access external catalogue

For detailed API documentation, refer to controller files in `src/KeeperData.Bridge/Controllers/`.

## Deployment

### Docker

Build and push image:

```bash
docker build -t keeper-data-bridge:latest .
docker push your-registry/keeper-data-bridge:latest
```

### CDP Environments

For deployment to CDP environments:

1. Ensure all required environment variables are configured
2. MongoDB credentials are automatically injected
3. Follow [CDP deployment documentation](https://github.com/DEFRA/cdp-local-environment)

### Kubernetes

Service can be deployed to Kubernetes clusters. Configuration examples:

```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
```

## Architecture

### Layered Architecture

```
Controllers (API endpoints)
    ↓
Application Layer (Use cases, validation)
    ↓
Domain Layer (Business logic, entities)
    ↓
Infrastructure Layer (Data access, external services)
```

### Key Components

- **Controllers** - HTTP request handlers
- **Services** - Application business logic
- **Repository Pattern** - Data access abstraction
- **Middleware** - Cross-cutting concerns (authentication, exception handling)
- **Authentication** - API Key and No-Auth handlers

## Troubleshooting

### MongoDB Connection Issues

```bash
# Verify MongoDB is running
docker compose ps mongodb

# Check connection
mongosh mongodb://localhost:27017

# View logs
docker compose logs mongodb
```

### Port Already in Use

If ports are already in use:

```bash
# Find process using port
lsof -i :5000

# Change port in launch settings or environment variable
ASPNETCORE_URLS=http://localhost:5002
```

### Tests Failing

```bash
# Clean and rebuild
dotnet clean
dotnet restore
dotnet build
dotnet test

# Run with verbose output
dotnet test -v normal
```

### Docker Compose Issues

```bash
# Remove all containers and volumes
docker compose down -v

# Rebuild from scratch
docker compose up --build -d

# View service logs
docker compose logs -f service-name
```

## Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make your changes and commit: `git commit -am 'Add feature'`
3. Push to the branch: `git push origin feature/your-feature`
4. Submit a Pull Request

**Code Standards:**
- Follow C# coding conventions
- Write unit tests for new features
- Run `dotnet format` before committing
- Ensure all tests pass
- Update documentation as needed

## Dependabot

Security and dependency updates are managed via Dependabot. To enable:

1. Rename `.github/example.dependabot.yml` to `.github/dependabot.yml`
2. Customize settings as needed
3. Dependabot will create PRs for available updates

## Licence

This project is licensed under the **Open Government Licence v3.0 (OGL)**

The Open Government Licence was developed by the Controller of Her Majesty's Stationery Office (HMSO) to enable information providers in the public sector to license the use and re-use of their information under a common open licence.

It is designed to encourage use and re-use of information freely and flexibly, with only a few conditions.

See the [Licence page](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) for full details.

---

**Need Help?** Open an issue on [GitHub](https://github.com/DEFRA/ls-keeper-data-bridge-backend/issues) or contact the development team.
