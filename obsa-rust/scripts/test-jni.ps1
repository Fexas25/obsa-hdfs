param([string]$JavaHome = $env:JAVA_HOME)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$java = if ($JavaHome) { Join-Path $JavaHome 'bin\java.exe' } else { (Get-Command java).Source }
$javac = if ($JavaHome) { Join-Path $JavaHome 'bin\javac.exe' } else { (Get-Command javac).Source }
$classes = Join-Path $projectRoot 'target\java-smoke'
New-Item -ItemType Directory -Force -Path $classes | Out-Null
& $javac -encoding UTF-8 -d $classes (Join-Path $projectRoot 'java\org\apache\hadoop\fs\obs\NativeObs.java') (Join-Path $projectRoot 'java\tests\NativeSmokeTest.java')
if ($LASTEXITCODE -ne 0) { throw 'Java smoke compilation failed' }
& $java "-Djava.library.path=$projectRoot\target\debug" -cp $classes NativeSmokeTest
if ($LASTEXITCODE -ne 0) { throw 'JNI smoke test failed' }

& $javac -encoding UTF-8 -cp $classes -d $classes (Join-Path $projectRoot 'java\tests\NativeErrorSmokeTest.java')
if ($LASTEXITCODE -ne 0) { throw 'JNI error smoke compilation failed' }
& $java "-Djava.library.path=$projectRoot\target\debug" -cp $classes NativeErrorSmokeTest
if ($LASTEXITCODE -ne 0) { throw 'JNI error smoke failed' }
