param(
    [Parameter(Mandatory=$true, Position=0)][string]$OutputPath,
    [Parameter(Mandatory=$false)][string]$Configuration = "Release"
)

$OutputPath = [System.IO.Path]::GetFullPath($OutputPath)
$binPath = (Join-Path $OutputPath "bin")
dotnet publish -c $Configuration -o $binPath
Copy-Item (Join-Path $PSScriptRoot "scripts" "*") $OutputPath -Recurse -Force