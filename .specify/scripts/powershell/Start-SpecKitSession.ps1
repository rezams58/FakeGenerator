# Launch an agent with process-local feature context; -CheckOnly never starts an agent.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateSet('codex','claude','gemini')][string]$Agent,
    [Parameter(Mandatory)][string]$FeatureDirectory,
    [switch]$CheckOnly,
    [string[]]$AgentArguments = @()
)
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../../..'))
$state = Get-Content -LiteralPath (Join-Path $repoRoot '.specify/integration.json') -Raw | ConvertFrom-Json
if ($Agent -notin $state.installed_integrations) {
    throw "Integration '$Agent' is not installed. Install it with: specify integration install $Agent --script ps"
}
if (-not [IO.Path]::IsPathRooted($FeatureDirectory)) {
    $FeatureDirectory = Join-Path $repoRoot $FeatureDirectory
}
$featurePath = (Resolve-Path -LiteralPath $FeatureDirectory -ErrorAction Stop).Path
if (-not (Test-Path -LiteralPath (Join-Path $featurePath 'spec.md') -PathType Leaf)) {
    throw 'FeatureDirectory must contain spec.md. Create a feature first (see docs/speckit-multi-agent.md).'
}
$context = @{
    SPECIFY_INIT_DIR = $repoRoot
    SPECIFY_FEATURE_DIRECTORY = $featurePath
    SPECIFY_FEATURE = Split-Path $featurePath -Leaf
    SPECIFY_FEATURE_NO_PERSIST = '1'
}
$previous = @{}
$previousLocation = Get-Location
try {
    foreach ($key in $context.Keys) {
        $previous[$key] = [Environment]::GetEnvironmentVariable($key, 'Process')
        [Environment]::SetEnvironmentVariable($key, $context[$key], 'Process')
    }
    Set-Location -LiteralPath $repoRoot
    if ($CheckOnly) {
        & (Join-Path $repoRoot '.specify/scripts/powershell/check-prerequisites.ps1') -Json -PathsOnly
        return
    }
    $executable = Get-Command $Agent -ErrorAction Stop
    & $executable @AgentArguments
    if ($LASTEXITCODE -ne 0) { throw "Agent exited with code $LASTEXITCODE" }
} finally {
    foreach ($key in $previous.Keys) {
        if ($null -eq $previous[$key]) {
            Remove-Item -LiteralPath "Env:$key" -ErrorAction SilentlyContinue
        } else {
            [Environment]::SetEnvironmentVariable($key, $previous[$key], 'Process')
        }
    }
    Set-Location -LiteralPath $previousLocation.Path
}
