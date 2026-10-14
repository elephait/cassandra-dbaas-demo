param([string]$JarPath, [string]$ProjectDirectory, [string]$JdbcUrl='jdbc:postgresql://localhost:15433/postgres')
$ErrorActionPreference='Stop'
$smokeLogs=Join-Path $PSScriptRoot '.smoke'
New-Item -ItemType Directory -Path $smokeLogs -Force | Out-Null
$processes=@()
try {
    foreach ($port in @(18088,18089)) {
        $arguments=@('-Xmx256m','-jar',$JarPath,"--server.port=$port","--spring.datasource.url=$JdbcUrl",'--spring.datasource.username=postgres','--spring.datasource.password=queue-demo-only','--spring.datasource.driver-class-name=org.postgresql.Driver')
        $processes+=Start-Process -FilePath (Get-Command java).Source -ArgumentList $arguments -WorkingDirectory $ProjectDirectory -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $smokeLogs "$port.log") -RedirectStandardError (Join-Path $smokeLogs "$port.err")
    }
    foreach ($port in @(18088,18089)) {
        $deadline=(Get-Date).AddSeconds(90)
        do {
            Start-Sleep -Seconds 2
            try { $health=Invoke-RestMethod "http://localhost:$port/clusters/default/health" -TimeoutSec 3 } catch { $health=$null }
        } while ((!$health -or $health.up -ne 3) -and (Get-Date) -lt $deadline)
        if (!$health -or $health.up -ne 3) { throw "Instance $port did not connect to cluster; inspect $smokeLogs" }
        Write-Output "Instance $port connected to three replicas"
    }
    $headers=@{'X-Admin-Token'='local-demo-only';'Idempotency-Key'=[guid]::NewGuid().ToString()}
    Invoke-RestMethod http://localhost:18088/clusters/default/keyspaces/queue_smoke -Method Post -Headers $headers | Out-Null
    $body='{"keyspace":"queue_smoke","approved":true}'
    $first=Invoke-RestMethod http://localhost:18088/clusters/default/repair -Method Post -Headers $headers -ContentType application/json -Body $body
    $replay=Invoke-RestMethod http://localhost:18089/clusters/default/repair -Method Post -Headers $headers -ContentType application/json -Body $body
    if ($first.id -ne $replay.id) { throw 'Two instances returned different operations for the same request' }
    Write-Output "Cross-instance idempotency passed: $($first.id)"
    $deadline=(Get-Date).AddMinutes(3)
    do {
        Start-Sleep -Seconds 3
        $status=Invoke-RestMethod "http://localhost:18089/operations/$($first.id)" -Headers $headers
    } while ($status.state -in @('QUEUED','CLAIMED','RUNNING') -and (Get-Date) -lt $deadline)
    if ($status.state -ne 'SUCCEEDED') { throw "Repair did not succeed: $($status | ConvertTo-Json -Compress)" }
    Write-Output 'PASS: two Java processes, shared PostgreSQL, same receipt, real Cassandra repair succeeded'
} finally {
    foreach ($process in $processes) { if (!$process.HasExited) { Stop-Process -Id $process.Id -Force } }
}
