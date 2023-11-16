param(
    [Parameter(Mandatory=$true)][string]$OutputPath,
    [Parameter(Mandatory=$false)][string]$Configuration = "Release"
)

function Get-ConfigFile([string]$Name)
{
    $path = Join-Path $OutputPath $Name
    if (Test-Path $path) {
        Get-Content $path
    }
}

function Set-ConfigFile([string]$Name, [string[]]$Content)
{
    if ($Content) {
        $path = Join-Path $OutputPath $Name
        Write-Host "Restoring $Name"
        $Content | Set-Content $path
    }
}

$configFiles = "bin/common.config","bin/dfs.config","bin/jet.config","support/Get-JumboConfig.ps1","deploy/group","deploy/masters","deploy/nodes"
$configContent = @{}
foreach ($file in $configFiles) {
    $configContent[$file] = Get-ConfigFile $file
}

#dotnet clean -c $Configuration
$OutputPath = [System.IO.Path]::GetFullPath($OutputPath)
$binPath = (Join-Path $OutputPath "bin")
$nugetPath = Join-Path $OutputPath "nuget"
dotnet build -c $Configuration /p:ContinuousIntegrationBuild=true
if ($LASTEXITCODE -ne 0) {
    throw "Build failed."
}

Remove-Item $binPath -Recurse

$publishProjects = "NameServer","DataServer","DfsShell","DfsWeb","JobServer","TaskServer","TaskHost","JetShell","JetWeb","Ookii.Jumbo.Jet.Samples"
foreach($project in $publishProjects) {
    dotnet publish $project --no-build -c $Configuration --output $binPath
}

$packProjects = "Ookii.Jumbo","Ookii.Jumbo.Dfs","Ookii.Jumbo.Jet"
foreach ($project in $packProjects) {
    dotnet pack $project --no-build -c $configuration --output $nugetPath
}

Copy-Item (Join-Path $PSScriptRoot "scripts" "*") $OutputPath -Recurse -Force
Copy-Item (Join-Path $PSScriptRoot "*.config") $binPath -Force
foreach ($file in $configFiles) {
    Set-ConfigFile $file $configContent[$file]
}
