param(
    [Parameter(Mandatory = $true)][string]$TestExecutable,
    [ValidateRange(1, 1000000)][int[]]$Entries = @(100000, 1000000),
    [ValidateRange(1, 20)][int]$Trials = 3,
    [switch]$ReverseOrder,
    [string]$OutputDirectory
)
$ErrorActionPreference = 'Stop'
if ($env:OS -ne 'Windows_NT') { throw 'This probe reports Windows working-set counters.' }
if (@($Entries | Sort-Object -Unique).Count -ne $Entries.Count) {
    throw 'Entry counts must be unique so per-case logs cannot overwrite each other.'
}
$executable = (Resolve-Path -LiteralPath $TestExecutable).Path
if (!(Test-Path -LiteralPath $executable -PathType Leaf)) { throw 'Test executable is required.' }
$projectRoot = Split-Path $PSScriptRoot -Parent
if (!$OutputDirectory) {
    $OutputDirectory = Join-Path $projectRoot ('target/listing-memory-' + [DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss-fff'))
}
if (Test-Path -LiteralPath $OutputDirectory) { throw 'Use a new output directory to avoid mixing runs.' }
$outputRoot = (New-Item -ItemType Directory -Path $OutputDirectory).FullName
$summaries = @()
$variants = if ($ReverseOrder) { @('paged', 'eager') } else { @('eager', 'paged') }
foreach ($count in $Entries) {
    foreach ($variant in $variants) {
        $start = New-Object System.Diagnostics.ProcessStartInfo
        $start.FileName = $executable
        $start.Arguments = 'benchmark_paginated_listing --ignored --exact --nocapture --test-threads=1'
        $start.WorkingDirectory = $projectRoot
        $start.UseShellExecute = $false
        $start.CreateNoWindow = $true
        $start.RedirectStandardOutput = $true
        $start.RedirectStandardError = $true
        $start.EnvironmentVariables['OBS_LIST_BENCH_ENTRIES'] = [string]$count
        $start.EnvironmentVariables['OBS_LIST_BENCH_TRIALS'] = [string]$Trials
        $start.EnvironmentVariables['OBS_LIST_BENCH_VARIANT'] = $variant
        $child = New-Object System.Diagnostics.Process
        $child.StartInfo = $start
        $peak = 0L
        $started = $false
        $watch = [System.Diagnostics.Stopwatch]::StartNew()
        try {
            if (!$child.Start()) { throw 'Cannot start listing benchmark.' }
            $started = $true
            $stdout = $child.StandardOutput.ReadToEndAsync()
            $stderr = $child.StandardError.ReadToEndAsync()
            while (!$child.WaitForExit(100)) {
                $child.Refresh()
                $peak = [Math]::Max($peak, $child.PeakWorkingSet64)
            }
            $watch.Stop()
            $text = $stdout.GetAwaiter().GetResult()
            $errors = $stderr.GetAwaiter().GetResult()
            [IO.File]::WriteAllText((Join-Path $outputRoot "$count-$variant.log"), $text)
            [IO.File]::WriteAllText((Join-Path $outputRoot "$count-$variant.stderr.log"), $errors)
            if ($child.ExitCode -ne 0) { throw "Listing benchmark failed: $count/$variant" }
            $rows = @($text -split '\r?\n' | ForEach-Object {
                if ($_ -match 'BENCH_JSON (\{.*\})$') { $Matches[1] | ConvertFrom-Json }
            })
            if ($rows.Count -ne $Trials) { throw 'Unexpected number of benchmark rows; rebuild the configured test executable.' }
            foreach ($row in $rows) {
                if ($row.entries -ne $count -or $row.variant -ne $variant -or
                    $row.requests -ne [Math]::Ceiling($count / 1000.0)) {
                    throw 'Benchmark workload or request counts do not match the requested configuration.'
                }
            }
            if ((@($rows.trial | Sort-Object -Unique) -join ',') -ne ((1..$Trials) -join ',')) {
                throw 'Benchmark trial indices are incomplete or duplicated.'
            }
            if ($peak -le 0) { throw 'No working-set sample captured; increase the workload.' }
            $summary = [ordered]@{
                entries = $count; variant = $variant; trials = $Trials
                peak_working_set_bytes_observed = $peak
                process_wall_ms = $watch.Elapsed.TotalMilliseconds
                process_cpu_ms = $child.TotalProcessorTime.TotalMilliseconds
                rows = $rows
            }
            $summaries += $summary
            $summary | ConvertTo-Json -Depth 6 -Compress
            $summaries | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 (Join-Path $outputRoot 'summary.json')
        } finally {
            if ($started -and !$child.HasExited) {
                $child.Kill()
                $child.WaitForExit()
            }
            $child.Dispose()
        }
    }
}
