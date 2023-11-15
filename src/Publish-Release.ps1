param(
    [Parameter(Mandatory=$true)][string]$OutputPath,
    [Parameter(Mandatory=$false)][string]$Configuration = "Release"
)

function Get-ConfigFile([string]$Name)
{
    $path = Join-Path $binPath $Name
    if (Test-Path $path) {
        Get-Content $path
    }
}

function Set-ConfigFile([string]$Name, [string[]]$Content)
{
    $path = Join-Path $binPath $Name
    if ($Content) {
        Write-Host "Restoring $Name"
        $Content | Set-Content $path
    } else {
        Write-Host "Copying default $Name"
        Copy-Item "$PSScriptRoot/$Name" $path
    }
}

dotnet clean -c $Configuration
$OutputPath = [System.IO.Path]::GetFullPath($OutputPath)
$binPath = (Join-Path $OutputPath "bin")
$nugetPath = Join-Path $OutputPath "nuget"
dotnet build -c $Configuration /p:ContinuousIntegrationBuild=true
if ($LASTEXITCODE -ne 0) {
    throw "Build failed."
}

$commonConfig = Get-ConfigFile "common.config"
$dfsConfig = Get-ConfigFile "dfs.config"
$jetConfig = Get-ConfigFile "jet.config"

Remove-Item $binPath -Recurse

$publishProjects = "NameServer","DataServer","DfsShell","DfsWeb","JobServer","TaskServer","TaskHost","JetShell","JetWeb","Ookii.Jumbo.Jet.Samples"
foreach($project in $publishProjects)
{
    dotnet publish $project --no-build -c $Configuration --output $binPath
}

$packProjects = "Ookii.Jumbo","Ookii.Jumbo.Dfs","Ookii.Jumbo.Jet"
foreach ($project in $packProjects)
{
    dotnet pack $project --no-build -c $configuration --output $nugetPath
}

Copy-Item (Join-Path $PSScriptRoot "scripts" "*") $OutputPath -Recurse -Force

Set-ConfigFile "common.config" $commonConfig
Set-ConfigFile "dfs.config" $dfsConfig
Set-ConfigFile "jet.config" $jetConfig
