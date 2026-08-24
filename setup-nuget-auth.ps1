<#
.SYNOPSIS
    Configures credentials for the DEFRA GitHub Packages NuGet feed in the current user's NuGet.Config.

.DESCRIPTION
    Writes environment variable references (not literal values) so the PAT never lands on disk.
    NuGet expands %VAR% at restore time, so GITHUB_DEFRA_USERNAME and GITHUB_PAT_DEFRA_PACKAGES_READ
    must be set persistently (User scope on Windows, shell profile elsewhere).

    Run once per machine, then rebuild:  dotnet restore

.EXAMPLE
    ./setup-nuget-auth.ps1
#>
[CmdletBinding()]
param(
    [string]$SourceName = 'DEFRA',
    [string]$SourceUrl = 'https://nuget.pkg.github.com/DEFRA/index.json',
    [string]$ConfigFile
)

$ErrorActionPreference = 'Stop'

$userVar = 'GITHUB_DEFRA_USERNAME'
$patVar = 'GITHUB_PAT_DEFRA_PACKAGES_READ'

foreach ($name in $userVar, $patVar) {
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name))) {
        throw "Environment variable $name is not set. Set it (User scope) and restart the shell, e.g.:`n" +
              "  [Environment]::SetEnvironmentVariable('$name','<value>','User')"
    }
}

if (-not $ConfigFile) {
    $ConfigFile = if ($env:APPDATA) {
        Join-Path $env:APPDATA 'NuGet\NuGet.Config'
    }
    else {
        Join-Path $HOME '.nuget/NuGet/NuGet.Config'
    }
}

$configDir = Split-Path -Parent $ConfigFile
if (-not (Test-Path $configDir)) {
    New-Item -ItemType Directory -Path $configDir -Force | Out-Null
}

$sources = @(dotnet nuget list source --configfile $ConfigFile)
if ($LASTEXITCODE -ne 0) {
    throw "dotnet nuget list source failed for $ConfigFile"
}

$verb = if ($sources -match "^\s*\d+\.\s+$([regex]::Escape($SourceName))\s") { 'update' } else { 'add' }

$dotnetArgs = @('nuget', $verb, 'source')
$dotnetArgs += if ($verb -eq 'add') { @($SourceUrl, '--name', $SourceName) } else { $SourceName }
$dotnetArgs += @(
    '--username', "%$userVar%"
    '--password', "%$patVar%"
    '--store-password-in-clear-text'
    '--configfile', $ConfigFile
)

Write-Host "Configuring '$SourceName' credentials in $ConfigFile ($verb)..."
dotnet @dotnetArgs
if ($LASTEXITCODE -ne 0) {
    throw "dotnet nuget $verb source failed"
}

Write-Host "Done. Run 'dotnet restore' to verify." -ForegroundColor Green
