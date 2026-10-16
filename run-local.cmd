<# : batch part (cmd.exe runs these lines; PowerShell sees a comment)
@echo off
set "DBAAS_HOME=%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "& ([scriptblock]::Create([IO.File]::ReadAllText('%~f0'))) %*"
exit /b %ERRORLEVEL%
#>
# PowerShell part. cmd.exe launches it with a process-scoped execution-policy bypass, so it runs whatever
# the machine's policy is.
$ErrorActionPreference = 'Stop'
Push-Location $env:DBAAS_HOME
try {
    if (-not (Test-Path target/dbaas-demo-1.0.0.jar)) { throw 'Build the project before running the local application' }
    # Run a copy, so a later build can replace target/ without breaking the running application.
    New-Item -ItemType Directory -Force .local | Out-Null
    try { Copy-Item target/dbaas-demo-1.0.0.jar .local/dbaas-demo.jar -Force }
    catch { throw 'The local application is already running; stop it before starting another' }
    & java -cp target/classes demo.platform.repair.jmx.LocalJmxDiscovery .local/jmx-local.properties
    if ($LASTEXITCODE -ne 0) { throw 'Could not discover local JMX endpoints; start the Compose cluster first' }
    & java -Xmx384m -jar .local/dbaas-demo.jar '--spring.profiles.active=local' '--spring.config.additional-location=file:.local/jmx-local.properties'
} finally { Pop-Location }
