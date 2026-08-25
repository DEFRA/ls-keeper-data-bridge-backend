param(
    [string]$ImageName = "keeperdata-test",
    [string]$Tag = "latest",
    [string]$ContextPath = ".",
    [string]$Dockerfile = "Dockerfile",
    [string]$EnvVarName = "GITHUB_PAT_DEFRA_PACKAGES_READ",
    [switch]$NoCache
)

$ErrorActionPreference = "Stop"

$resolvedContextPath = if ([System.IO.Path]::IsPathRooted($ContextPath)) {
    $ContextPath
} else {
    Join-Path $PSScriptRoot $ContextPath
}

$resolvedDockerfile = if ([System.IO.Path]::IsPathRooted($Dockerfile)) {
    $Dockerfile
} else {
    Join-Path $PSScriptRoot $Dockerfile
}

if (-not (Test-Path "Env:$EnvVarName")) {
    throw "Environment variable '$EnvVarName' is not set. Export it before running this script."
}

$argsList = @(
    "build"
)

if ($NoCache) {
    $argsList += "--no-cache"
}

$argsList += @(
    "--secret",
    "id=nuget_auth_token,env=$EnvVarName",
    "-t",
    "${ImageName}:${Tag}",
    "-f",
    $resolvedDockerfile,
    $resolvedContextPath
)

Write-Host "Building Docker image '${ImageName}:${Tag}' from '$resolvedContextPath' using secret '$EnvVarName' mapped to 'nuget_auth_token'."
& docker @argsList

if ($LASTEXITCODE -ne 0) {
    throw "Docker build failed with exit code $LASTEXITCODE."
}
