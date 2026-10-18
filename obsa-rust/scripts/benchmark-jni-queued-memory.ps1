param(
    [string]$JavaHome = $env:JAVA_HOME,
    [ValidateRange(1, 100)][int]$Rounds = 3,
    [string]$NativeLibraryPath
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$java = if ($JavaHome) { Join-Path $JavaHome 'bin/java.exe' } else { (Get-Command java).Source }
$javac = if ($JavaHome) { Join-Path $JavaHome 'bin/javac.exe' } else { (Get-Command javac).Source }
if (!$NativeLibraryPath) { $NativeLibraryPath = Join-Path $projectRoot 'target/release' }
$NativeLibraryPath = (Resolve-Path -LiteralPath $NativeLibraryPath).Path
if (!(Test-Path -LiteralPath (Join-Path $NativeLibraryPath 'obs_jni.dll') -PathType Leaf)) {
    throw 'Build the release JNI library before benchmarking.'
}
$classes = Join-Path $projectRoot 'target/java-queued-memory-benchmark'
New-Item -ItemType Directory -Force -Path $classes | Out-Null
& $javac -encoding UTF-8 -d $classes (Join-Path $projectRoot 'java/org/apache/hadoop/fs/obs/NativeObs.java') (Join-Path $projectRoot 'java/benchmarks/JniQueuedMemoryProbe.java')
if ($LASTEXITCODE -ne 0) { throw 'Queued memory benchmark compilation failed' }
foreach ($round in 1..$Rounds) {
    foreach ($threads in @(1, 4, 8, 16)) {
        foreach ($mode in @('legacy', 'into')) {
            & $java -Xms128m -Xmx128m "-Djava.library.path=$NativeLibraryPath" -cp $classes JniQueuedMemoryProbe $threads $mode
            if ($LASTEXITCODE -ne 0) { throw "Queued memory benchmark failed: round=$round threads=$threads mode=$mode" }
        }
    }
}
