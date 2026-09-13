<#
.SYNOPSIS
    Run N independent single-node Elasticsearch clusters that can remote-reindex
    from one another.

.DESCRIPTION
    Each cluster is a separate docker compose project built from the same
    docker-compose.yml, so compose gives each one its own network, volume and
    container names. They share one external network purely so that remote
    reindex can reach across.

.EXAMPLE
    .\clusters.ps1 up 3     # start three clusters and print their URLs
    .\clusters.ps1 urls     # print the URLs of whatever is running
    .\clusters.ps1 test     # prove remote reindex works between two of them
    .\clusters.ps1 down     # remove them (add -Volumes to drop the data too)
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('up', 'urls', 'test', 'down', 'status')]
    [string]$Command = 'urls',

    [Parameter(Position = 1)]
    [int]$Count = 3,

    [switch]$Volumes
)

$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$Prefix = 'es'

$Network = $env:SHARED_NETWORK
if (-not $Network) { $Network = 'es-shared' }

function Assert-LastExitCode {
    param([string]$What)
    if ($LASTEXITCODE -ne 0) { throw "$What failed (exit $LASTEXITCODE)" }
}

# The running clusters, discovered rather than assumed, so urls/down/test work
# without being told how many there are.
function Get-Clusters {
    # `docker compose ls` prints one JSON array, but PowerShell hands it back
    # as a string array, so join before parsing.
    $raw = (docker compose ls --all --format json) -join ''
    if (-not $raw) { return @() }

    # Assign before enumerating: in PowerShell 5.1 ConvertFrom-Json writes an
    # array to the pipeline as ONE object, so @(ConvertFrom-Json ...) yields a
    # single element holding the whole array.
    $projects = ConvertFrom-Json $raw

    $found = New-Object System.Collections.ArrayList
    foreach ($project in @($projects)) {
        if ($project.Name -match "^$Prefix-(\d+)$") {
            [void]$found.Add([PSCustomObject]@{
                Name   = $project.Name
                Index  = [int]$Matches[1]
                Status = $project.Status
            })
        }
    }
    @($found | Sort-Object -Property Index)
}

function Get-ClusterUrl {
    param([string]$Project)
    # Ask Docker what it actually published; works whether the port was pinned
    # via ES_PORT or assigned ephemerally.
    $line = @(docker compose -p $Project port es 9200 | Where-Object { $_ }) | Select-Object -First 1
    if (-not $line) { return $null }
    $hostPort = ($line -split ':')[-1]
    "http://localhost:$hostPort"
}

function Show-Urls {
    $clusters = Get-Clusters
    if ($clusters.Count -eq 0) {
        Write-Host "no clusters running (start some with: .\clusters.ps1 up 3)"
        return
    }
    Write-Host ""
    $rows = foreach ($c in $clusters) {
        [PSCustomObject]@{
            Cluster  = $c.Name
            Url      = Get-ClusterUrl $c.Name
            Internal = "http://$($c.Name):9200"
            Status   = $c.Status
        }
    }
    $rows | Format-Table -AutoSize | Out-String | Write-Host
    Write-Host "Url      = from Windows.  Internal = from inside another cluster (use this as a reindex source)."
}

function Initialize-Network {
    $existing = docker network ls --filter "name=^$Network$" --format '{{.Name}}'
    if (-not $existing) {
        docker network create $Network | Out-Null
        Assert-LastExitCode "docker network create $Network"
        Write-Host "created shared network '$Network'"
    }
}

switch ($Command) {

    'up' {
        if ($Count -lt 1) { throw "Count must be at least 1" }
        Initialize-Network
        for ($i = 1; $i -le $Count; $i++) {
            $name = "$Prefix-$i"
            Write-Host "starting $name ..."
            $env:CLUSTER_NAME = $name
            # --wait blocks on the healthcheck, so when this returns the
            # cluster is actually serving, not merely started.
            docker compose -p $name up -d --wait
            Assert-LastExitCode "docker compose -p $name up"
        }
        Remove-Item Env:CLUSTER_NAME -ErrorAction SilentlyContinue
        Show-Urls
    }

    'urls'   { Show-Urls }

    'status' {
        Get-Clusters | Format-Table Name, Status -AutoSize | Out-String | Write-Host
    }

    'down' {
        $clusters = Get-Clusters
        if ($clusters.Count -eq 0) { Write-Host "nothing to remove"; break }
        foreach ($c in $clusters) {
            Write-Host "removing $($c.Name) ..."
            $env:CLUSTER_NAME = $c.Name
            if ($Volumes) { docker compose -p $c.Name down -v }
            else          { docker compose -p $c.Name down }
        }
        Remove-Item Env:CLUSTER_NAME -ErrorAction SilentlyContinue
        if (-not $Volumes) { Write-Host "data volumes kept; add -Volumes to drop them" }
    }

    'test' {
        # Proves the thing that is easy to get wrong: a remote reindex pulling
        # across cluster boundaries.
        $clusters = Get-Clusters
        if ($clusters.Count -lt 2) { throw "need at least 2 clusters; run: .\clusters.ps1 up 2" }

        $source = $clusters[0].Name
        $dest   = $clusters[1].Name
        $srcUrl = Get-ClusterUrl $source
        $dstUrl = Get-ClusterUrl $dest

        Write-Host "source      $source ($srcUrl)"
        Write-Host "destination $dest ($dstUrl)"
        Write-Host ""

        # Start from a clean slate, or a second run just piles documents on
        # top of the first and the counts stop proving anything.
        foreach ($pair in @(@($srcUrl, 'people'), @($dstUrl, 'people_copy'))) {
            try { Invoke-RestMethod -Method Delete -Uri "$($pair[0])/$($pair[1])" | Out-Null }
            catch { }   # 404 when it does not exist yet
        }

        Write-Host "seeding 3 documents into '$source/people' ..."
        $bulk = @(
            '{"index":{}}', '{"name":"ada"}',
            '{"index":{}}', '{"name":"grace"}',
            '{"index":{}}', '{"name":"edsger"}'
        ) -join "`n"
        $bulk += "`n"
        Invoke-RestMethod -Method Post -Uri "$srcUrl/people/_bulk?refresh=true" `
            -ContentType 'application/x-ndjson' -Body $bulk | Out-Null

        $srcCount = (Invoke-RestMethod "$srcUrl/people/_count").count
        Write-Host "source now holds $srcCount documents"

        # The remote host is the *internal* address: the destination container
        # resolves it over the shared docker network, not via the host port.
        Write-Host "reindexing $source -> $dest over http://${source}:9200 ..."
        $body = @{
            source = @{
                remote = @{ host = "http://${source}:9200" }
                index  = 'people'
            }
            dest = @{ index = 'people_copy' }
        } | ConvertTo-Json -Depth 6

        $result = Invoke-RestMethod -Method Post -Uri "$dstUrl/_reindex?refresh=true" `
            -ContentType 'application/json' -Body $body

        $dstCount = (Invoke-RestMethod "$dstUrl/people_copy/_count").count
        Write-Host ""
        Write-Host "reindex created $($result.created) documents; destination holds $dstCount"

        if ($dstCount -eq $srcCount -and $srcCount -gt 0) {
            Write-Host "PASS - remote reindex works across clusters" -ForegroundColor Green
        }
        else {
            throw "FAIL - expected $srcCount documents at the destination, found $dstCount"
        }
    }
}
