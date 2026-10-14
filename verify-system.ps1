param(
    [string]$JdbcUrl = 'jdbc:postgresql://localhost:15433/postgres',
    [int]$Port = 18090,
    [switch]$Recovery
)
$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$logs = Join-Path $root '.smoke'
New-Item -ItemType Directory -Path $logs -Force | Out-Null
$base = "http://localhost:$Port"
$headers = @{'X-Admin-Token' = 'local-demo-only'}
$process = $null

function Request([string]$Path, [int]$Expected = 200, [string]$Method = 'GET',
                 $Body = $null, [hashtable]$Auth = $headers) {
    $args = @{Uri="$base$Path"; Method=$Method; Headers=$Auth; TimeoutSec=30; SkipHttpErrorCheck=$true}
    if ($null -ne $Body) {
        $args.ContentType = 'application/json'
        $args.Body = $Body | ConvertTo-Json -Depth 12 -Compress
    }
    $response = Invoke-WebRequest @args
    if ([int]$response.StatusCode -ne $Expected) {
        throw "$Method $Path expected $Expected, got $($response.StatusCode): $($response.Content)"
    }
    if ($response.Content) { return ConvertFrom-Json -InputObject $response.Content }
}

try {
    $arguments = @('-Xmx256m', '-jar', (Join-Path $root 'target/dbaas-demo-1.0.0.jar'),
        "--server.port=$Port", "--spring.datasource.url=$JdbcUrl",
        '--spring.datasource.username=postgres', '--spring.datasource.password=queue-demo-only',
        '--spring.datasource.driver-class-name=org.postgresql.Driver')
    $process = Start-Process -FilePath (Get-Command java).Source -ArgumentList $arguments `
        -WorkingDirectory $root -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $logs 'system.log') `
        -RedirectStandardError (Join-Path $logs 'system.err')
    $deadline = (Get-Date).AddSeconds(90)
    do {
        Start-Sleep -Seconds 2
        try { $health = Request '/clusters/default/health' } catch { $health = $null }
    } while ((!$health -or $health.up -ne 3) -and (Get-Date) -lt $deadline)
    if (!$health -or $health.up -ne 3) { throw 'Application failed to connect to Cassandra' }

    Request '/clusters' 403 -Auth @{} | Out-Null
    Request '/clusters' 403 -Auth @{'X-Admin-Token'='wrong'} | Out-Null
    Request '/clusters/missing/health' 404 | Out-Null
    Request '/clusters/default/keyspaces?limit=0' 400 | Out-Null
    Request '/clusters/default/keyspaces?limit=201' 400 | Out-Null
    Request '/operations/not-a-uuid' 400 | Out-Null
    Request ('/operations/' + [guid]::NewGuid()) 404 | Out-Null
    Request '/clusters/default/keyspaces/UPPER' 400 'POST' | Out-Null
    Request '/clusters/default/keyspaces/system' 400 'POST' | Out-Null
    Request '/clusters/default/repair' 400 'POST' @{keyspace='queue_smoke'; approved=$false} | Out-Null
    Request '/clusters/default/repair' 400 'POST' @{keyspace='queue_smoke'; approved=$true} | Out-Null
    Request '/clusters/default/keyspaces/queue_smoke/probe' 400 'POST' @{value=('x' * 1025)} | Out-Null
    Write-Output 'PASS: authentication, identifiers, limits, request validation, and missing resources'

    # A separate logical cluster verifies configuration persistence and quota isolation.
    $clusterId = 'verify_' + [guid]::NewGuid().ToString('N').Substring(0,8)
    $default = @(Request '/clusters' | Where-Object id -eq 'default')[0]
    $configuration = @{
        contactPoints=$default.contactPoints; datacenter=$default.datacenter
        expectedNodes=$default.expectedNodes; replicationFactor=$default.replicationFactor
        addressMappings=$default.addressMappings; requestTimeoutSeconds=10; reconnectSeconds=2
        maxKeyspaces=1; provisioningLeaseSeconds=6; enabled=$true
        repairPolicy=@{nodes=$default.repairPolicy.nodes; command=$default.repairPolicy.command; leaseMinutes=1}
    }
    $saved = Request "/clusters/$clusterId" 200 'PUT' $configuration
    $again = Request "/clusters/$clusterId" 200 'PUT' $configuration
    if ($saved.configVersion -ne $again.configVersion -or $saved.repairPolicy.version -ne $again.repairPolicy.version) {
        throw 'Identical configuration changed versions'
    }
    $deadline = (Get-Date).AddSeconds(45)
    do {
        Start-Sleep -Seconds 1
        $health = Request "/clusters/$clusterId/health"
    } while ($health.up -ne 3 -and (Get-Date) -lt $deadline)
    if ($health.up -ne 3) { throw 'New cluster session failed to connect' }
    $keyspace = $clusterId
    Request "/clusters/$clusterId/keyspaces/$keyspace" 200 'POST' | Out-Null
    Request "/clusters/$clusterId/keyspaces/$keyspace" 200 'POST' | Out-Null
    $listed = @(Request "/clusters/$clusterId/keyspaces")
    if ($listed.Count -ne 1 -or $listed[0].state -ne 'READY') { throw 'Provisioning did not persist READY' }
    Request "/clusters/$clusterId/keyspaces/${keyspace}_extra" 409 'POST' | Out-Null
    $probe = Request "/clusters/$clusterId/keyspaces/$keyspace/probe" 200 'POST' @{value='refactoring verification'}
    if ($probe.value -ne 'refactoring verification' -or $probe.consistency -ne 'LOCAL_QUORUM') { throw 'Quorum read-back failed' }
    Write-Output 'PASS: configuration replay, session creation, provisioning replay, quota, and real quorum read-back'

    $configuration.enabled = $false
    Request "/clusters/$clusterId" 200 'PUT' $configuration | Out-Null
    Request "/clusters/$clusterId/keyspaces/$keyspace" 404 'POST' | Out-Null
    $configuration.enabled = $true
    $configuration.contactPoints = @(@{host='127.0.0.1'; port=19042})
    $configuration.addressMappings = @()
    $configuration.expectedNodes = 1
    $configuration.replicationFactor = 1
    Request "/clusters/$clusterId" 200 'PUT' $configuration | Out-Null
    $unavailable = Request "/clusters/$clusterId/health"
    if ($unavailable.status -ne 'UNAVAILABLE') { throw 'Disconnected cluster did not report UNAVAILABLE' }
    Request "/clusters/$clusterId/keyspaces/$keyspace" 503 'POST' | Out-Null
    if ((Request '/clusters/default/health').up -ne 3) { throw 'Disconnected cluster affected default cluster' }
    Write-Output 'PASS: disabling, unavailable Cassandra, and session isolation'

    if ($Recovery) {
        & (Join-Path $root 'verify-recovery.ps1') -BaseUrl $base
    }
    Write-Output 'PASS: system API verification completed'
} finally {
    if ($process -and !$process.HasExited) { Stop-Process -Id $process.Id -Force }
}
