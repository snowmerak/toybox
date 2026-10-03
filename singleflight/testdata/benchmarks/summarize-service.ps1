param(
    [string]$InputPath = 'singleflight/testdata/benchmarks/windows-arm64-service.txt',
    [string]$OutputPath = 'singleflight/testdata/benchmarks/summary-service.json',
    [int]$ExpectedCases = 48,
    [int]$ExpectedSamples = 5
)

$ErrorActionPreference = 'Stop'
$invariant = [System.Globalization.CultureInfo]::InvariantCulture
$utf8 = New-Object System.Text.UTF8Encoding($false)
$lines = [System.IO.File]::ReadAllLines((Join-Path (Get-Location) $InputPath))
if (-not ($lines -contains 'PASS') -or ($lines | Where-Object { $_ -match '^FAIL' })) {
    throw 'Service benchmark did not pass.'
}
$required = @('ns/op', 'B/op', 'allocs/op', 'requests/s', 'clients', 'backend/op',
    'shared/op', 'unique/op', 'dochan/op', 'errors/op', 'slow-backend/op',
    'backend-mean-ns', 'backend-p95-ns', 'p50-ns', 'p95-ns', 'p99-ns',
    'latency-zero/op', 'backend-zero/exec')

function Median($values) {
    $sorted = @($values | Sort-Object)
    if ($sorted.Count -eq 0) { throw 'Empty metric.' }
    $middle = [int][math]::Floor($sorted.Count / 2)
    if ($sorted.Count % 2) { return $sorted[$middle] }
    return ($sorted[$middle - 1] + $sorted[$middle]) / 2
}

$rows = @()
foreach ($line in $lines) {
    if ($line -notmatch '^(BenchmarkServiceFlood/\S+)\s+(\d+)\s+(.*)$') { continue }
    $name = $matches[1]
    $requests = [long]::Parse($matches[2], $invariant)
    $rest = $matches[3]
    $metrics = @{}
    foreach ($metric in [regex]::Matches($rest, '([0-9.eE+-]+)\s+(\S+)')) {
        $key = $metric.Groups[2].Value
        if ($metrics.ContainsKey($key)) { throw "Duplicate metric: $name $key" }
        $metrics[$key] = [double]::Parse($metric.Groups[1].Value, $invariant)
    }
    foreach ($key in $required) {
        if (-not $metrics.ContainsKey($key)) { throw "Missing metric: $name $key" }
        if ([double]::IsNaN($metrics[$key]) -or [double]::IsInfinity($metrics[$key]) -or $metrics[$key] -lt 0) {
            throw "Invalid metric: $name $key"
        }
    }
    if ($requests -le 0 -or $metrics['errors/op'] -ne 0 -or
        $metrics['backend/op'] -gt 1 -or $metrics['backend/op'] + 0.0002 -lt $metrics['unique/op'] -or
        $metrics['shared/op'] -gt 1 -or $metrics['unique/op'] -gt 1 -or $metrics['dochan/op'] -gt 1 -or
        $metrics['p50-ns'] -gt $metrics['p95-ns'] -or $metrics['p95-ns'] -gt $metrics['p99-ns'] -or
        $metrics['requests/s'] -le 0 -or $metrics['ns/op'] -le 0 -or
        [math]::Abs($metrics['requests/s'] * $metrics['ns/op'] / 1e9 - 1) -gt 0.01) {
        throw "Inconsistent service metrics: $name"
    }
    $rows += [pscustomobject]@{ Name=$name; Requests=$requests; Metrics=$metrics }
}
$groups = @($rows | Group-Object Name)
if ($groups.Count -ne $ExpectedCases) { throw "Expected $ExpectedCases cases, found $($groups.Count)." }
$summary = @()
foreach ($group in $groups) {
    if ($group.Count -ne $ExpectedSamples) { throw "Wrong sample count: $($group.Name)" }
    $metrics = [ordered]@{}
    foreach ($key in $required) {
        $values = @($group.Group | ForEach-Object { $_.Metrics[$key] } | Sort-Object)
        $metrics[$key] = [ordered]@{ Median=(Median $values); Min=$values[0]; Max=$values[-1] }
    }
    $summary += [pscustomobject]@{
        Name=$group.Name
        Count=$group.Count
        Metrics=$metrics
        Samples=@($group.Group | Select-Object Requests,Metrics)
    }
}
[System.IO.File]::WriteAllText((Join-Path (Get-Location) $OutputPath), ($summary | ConvertTo-Json -Depth 8), $utf8)
[System.IO.File]::WriteAllLines((Join-Path (Get-Location) $InputPath), $lines, $utf8)
"Validated $($groups.Count) service cases with $ExpectedSamples samples each, zero errors, and all metrics."
