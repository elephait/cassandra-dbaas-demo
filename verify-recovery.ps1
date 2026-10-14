param([string]$BaseUrl='http://localhost:8088', [string]$AdminToken='local-demo-only')
$ErrorActionPreference='Stop'
if ($env:SPRING_PROFILES_ACTIVE -and $env:SPRING_PROFILES_ACTIVE -ne 'local') {
    throw 'verify-recovery.ps1 is local-only and cannot run with a non-local Spring profile'
}
Push-Location $PSScriptRoot
$stopped=$false
function Docker([string[]]$Arguments) {
    & docker.exe @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker failed: $Arguments" }
}
function Probe([string]$Phase, [hashtable]$Headers) {
    1..20 | ForEach-Object {
        $value="$Phase-$_"
        $result=Invoke-RestMethod "$BaseUrl/clusters/default/keyspaces/interview/probe" -Method Post -Headers $Headers -ContentType application/json -Body (@{value=$value}|ConvertTo-Json)
        if ($result.value -ne $value -or $result.consistency -ne 'LOCAL_QUORUM') { throw 'Probe mismatch' }
        Start-Sleep -Milliseconds 250
    }
    Write-Host "$Phase : 20 LOCAL_QUORUM writes and read-backs passed"
}
try {
    $headers=@{'X-Admin-Token'=$AdminToken}
    Invoke-RestMethod "$BaseUrl/clusters/default/keyspaces/interview" -Method Post -Headers $headers
    Probe 'healthy' $headers
    Docker @('compose','stop','cassandra3')
    $stopped=$true
    Probe 'one-node-down' $headers
    Invoke-RestMethod "$BaseUrl/clusters/default/health"
    Docker @('compose','start','cassandra3')
    $stopped=$false
    $deadline=(Get-Date).AddMinutes(10)
    do {
        Start-Sleep -Seconds 5
        $health=Invoke-RestMethod "$BaseUrl/clusters/default/health"
    } while ($health.up -ne 3 -and (Get-Date) -lt $deadline)
    if ($health.up -ne 3) { throw 'Node recovery timed out' }
    Probe 'recovered' $headers
    $headers['Idempotency-Key']=[guid]::NewGuid().ToString()
    $op=Invoke-RestMethod "$BaseUrl/clusters/default/repair" -Method Post -Headers $headers -ContentType application/json -Body '{"keyspace":"interview","approved":true}'
    $deadline=(Get-Date).AddMinutes(46)
    do {
        Start-Sleep -Seconds 3
        $status=Invoke-RestMethod "$BaseUrl/operations/$($op.id)" -Headers $headers
    } while ($status.state -in @('QUEUED','CLAIMED','RUNNING') -and (Get-Date) -lt $deadline)
    if ($status.state -ne 'SUCCEEDED') { throw "Repair did not succeed: $($status|ConvertTo-Json -Compress)" }
    Probe 'after-repair' $headers
    Write-Host 'PASS: failure, recovery, and repair verified'
} finally {
    if ($stopped) { & docker.exe compose start cassandra3 }
    Pop-Location
}


