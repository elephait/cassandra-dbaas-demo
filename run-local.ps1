$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    & mvn -B '-Dmaven.repo.local=.m2' package
    if ($LASTEXITCODE -ne 0) { throw 'Maven build failed' }
    & java -Xmx384m -jar target/cassandra-dbaas-demo-1.0.0.jar
} finally { Pop-Location }
