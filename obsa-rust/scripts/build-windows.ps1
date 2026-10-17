param([switch]$SkipTests, [switch]$BenchmarkListing, [switch]$BenchmarkCache, [switch]$BenchmarkHttp, [switch]$BenchmarkRange, [switch]$BenchmarkListValidation, [switch]$BenchmarkAppend, [switch]$Release)
$ErrorActionPreference = 'Stop'
if (([int]$BenchmarkListing.IsPresent + [int]$BenchmarkCache.IsPresent + [int]$BenchmarkHttp.IsPresent + [int]$BenchmarkRange.IsPresent + [int]$BenchmarkListValidation.IsPresent + [int]$BenchmarkAppend.IsPresent) -gt 1) { throw 'Select one benchmark at a time.' }
$projectRoot = Split-Path $PSScriptRoot -Parent
$vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
if (!(Test-Path -LiteralPath $vswhere)) { throw 'Visual Studio C++ Build Tools are required.' }
$vsRoot = & $vswhere -latest -products '*' -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
$msvcRoot = Get-ChildItem -LiteralPath (Join-Path $vsRoot 'VC\Tools\MSVC') -Directory | Sort-Object Name -Descending | Select-Object -First 1 -ExpandProperty FullName
$sdkRoot = (Get-ItemProperty -LiteralPath 'HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows Kits\Installed Roots').KitsRoot10
$sdkVersion = Get-ChildItem -LiteralPath (Join-Path $sdkRoot 'Lib') -Directory | Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName 'um\x64\kernel32.lib') } | Sort-Object Name -Descending | Select-Object -First 1 -ExpandProperty Name
if (!$sdkVersion) { throw 'Windows SDK x64 libraries are required.' }
$env:PATH = "$msvcRoot\bin\Hostx64\x64;" + $env:PATH
$env:LIB = "$msvcRoot\lib\x64;$sdkRoot\Lib\$sdkVersion\um\x64;$sdkRoot\Lib\$sdkVersion\ucrt\x64"
$env:INCLUDE = "$msvcRoot\include;$sdkRoot\Include\$sdkVersion\ucrt;$sdkRoot\Include\$sdkVersion\shared;$sdkRoot\Include\$sdkVersion\um"
$cargo = Get-Command cargo -ErrorAction SilentlyContinue
$cargoPath = if ($cargo) { $cargo.Source } else { Join-Path $env:USERPROFILE '.cargo\bin\cargo.exe' }
if ($BenchmarkAppend) {
    & $cargoPath test --locked --release --manifest-path (Join-Path $projectRoot 'Cargo.toml') -p obs-rust-core benchmark_append -- --ignored --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw 'append/rename benchmark failed' }
    return
}
if ($BenchmarkListValidation) {
    & $cargoPath test --locked --release --manifest-path (Join-Path $projectRoot 'Cargo.toml') -p obs-rust-core --lib benchmark_mutation_key_validation -- --ignored --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw 'list validation benchmark failed' }
    return
}
if ($BenchmarkRange) {
    & $cargoPath test --locked --release --manifest-path (Join-Path $projectRoot 'Cargo.toml') -p obs-rust-core --lib benchmark_range_collection -- --ignored --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw 'range collection benchmark failed' }
    return
}
if ($BenchmarkHttp) {
    & $cargoPath test --locked --release --manifest-path (Join-Path $projectRoot 'Cargo.toml') -p obs-rust-core --lib benchmark_http_frame_collection -- --ignored --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw 'HTTP collection benchmark failed' }
    return
}
if ($BenchmarkCache) {
    & $cargoPath test --locked --release --manifest-path (Join-Path $projectRoot 'Cargo.toml') -p obs-rust-core --test cache_latency -- --include-ignored --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw 'cache benchmark failed' }
    return
}
if ($BenchmarkListing) {
    & $cargoPath test --locked --release --manifest-path (Join-Path $projectRoot 'Cargo.toml') -p obs-rust-core --test filesystem benchmark_paginated_listing -- --ignored --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw 'listing benchmark failed' }
    return
}
$rustBuildProfileArgs = @()
if ($Release) { $rustBuildProfileArgs += '--release' }
& $cargoPath build --locked @rustBuildProfileArgs --workspace --manifest-path (Join-Path $projectRoot 'Cargo.toml')
if ($LASTEXITCODE -ne 0) { throw 'cargo build failed' }
if (!$SkipTests) {
    & $cargoPath test --locked @rustBuildProfileArgs --workspace --manifest-path (Join-Path $projectRoot 'Cargo.toml')
    if ($LASTEXITCODE -ne 0) { throw 'cargo test failed' }
}
