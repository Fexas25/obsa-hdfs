param(
    [Parameter(Mandatory=$true)][string]$FilesystemExe,
    [Parameter(Mandatory=$true)][string]$OutputDirectory,
    [ValidateRange(1,100)][int]$Batches=3,
    [switch]$Profile
)
$ErrorActionPreference='Stop'
$exe=(Resolve-Path -LiteralPath $FilesystemExe).Path
$output=[System.IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $output -Force | Out-Null
$names=@('OBS_APPEND_PROFILE','OBS_APPEND_BENCH_INPLACE','OBS_APPEND_BENCH_PREWARM',
    'OBS_APPEND_BENCH_RESERVE','OBS_APPEND_BENCH_NO_RENAME','OBS_APPEND_BENCH_STORE')
$saved=@{}
foreach($name in $names){$saved[$name]=[Environment]::GetEnvironmentVariable($name,'Process')}
try {
    $env:OBS_APPEND_BENCH_INPLACE='1'
    [Environment]::SetEnvironmentVariable('OBS_APPEND_PROFILE', $(if($Profile){'1'}else{$null}), 'Process')
    $stores=@('legacy','scan-global','range-global','range-object')
    for($batch=1;$batch -le $Batches;$batch++) {
        foreach($warm in @('cold','warm')) {
            [Environment]::SetEnvironmentVariable('OBS_APPEND_BENCH_PREWARM', $(if($warm -eq 'warm'){'1'}else{$null}), 'Process')
            foreach($capacity in @('grow','reserve')) {
                [Environment]::SetEnvironmentVariable('OBS_APPEND_BENCH_RESERVE', $(if($capacity -eq 'reserve'){'1'}else{$null}), 'Process')
                foreach($rename in @('rename','none')) {
                    [Environment]::SetEnvironmentVariable('OBS_APPEND_BENCH_NO_RENAME', $(if($rename -eq 'none'){'1'}else{$null}), 'Process')
                    for($offset=0;$offset -lt $stores.Count;$offset++) {
                        $store=$stores[($batch+$offset)%$stores.Count]
                        $env:OBS_APPEND_BENCH_STORE=$store
                        $log=Join-Path $output "$warm-$capacity-$rename-$store-$batch.log"
                        if(Test-Path -LiteralPath $log){throw "Refusing to overwrite existing benchmark: $log"}
                        & $exe --exact append_latency::benchmark_append_filesystem --ignored --nocapture *> $log
                        if($LASTEXITCODE -ne 0){throw "Benchmark failed; see $log"}
                        $rows=@(Get-Content -LiteralPath $log | Where-Object { $_.StartsWith('APPEND_BENCH ') } | ForEach-Object { $_.Substring(13) | ConvertFrom-Json })
                        if($rows.Count -ne 6){throw "Expected six samples: $log"}
                        if($Profile -and @($rows | Where-Object { $null -eq $_.task_operations }).Count){throw 'Profile requires an append-diagnostics build'}
                    }
                }
            }
        }
    }
} finally {
    foreach($name in $names){[Environment]::SetEnvironmentVariable($name,$saved[$name],'Process')}
}
