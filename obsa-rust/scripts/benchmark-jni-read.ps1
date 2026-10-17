param(
    [string]$JavaHome = $env:JAVA_HOME,
    [ValidateRange(1, 100)][int]$Rounds = 7
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$java = if ($JavaHome) { Join-Path $JavaHome 'bin/java.exe' } else { (Get-Command java).Source }
$javac = if ($JavaHome) { Join-Path $JavaHome 'bin/javac.exe' } else { (Get-Command javac).Source }
$nativeLibraryPath = Join-Path $projectRoot 'target/release'
if (!(Test-Path -LiteralPath (Join-Path $nativeLibraryPath 'obs_jni.dll') -PathType Leaf)) {
    throw 'Build the release JNI library before benchmarking.'
}
$classes = Join-Path $projectRoot 'target/java-read-benchmark'
New-Item -ItemType Directory -Force -Path $classes | Out-Null
& $javac -encoding UTF-8 -d $classes (Join-Path $projectRoot 'java/org/apache/hadoop/fs/obs/NativeObs.java') (Join-Path $projectRoot 'java/benchmarks/JniReadBenchmark.java')
if ($LASTEXITCODE -ne 0) { throw 'Read benchmark compilation failed' }
& $java -Xms256m -Xmx256m "-Djava.library.path=$nativeLibraryPath" -cp $classes JniReadBenchmark $Rounds
if ($LASTEXITCODE -ne 0) { throw 'Read benchmark failed' }
