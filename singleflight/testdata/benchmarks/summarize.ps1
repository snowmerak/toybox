param(
    [string]$InputPath = 'singleflight/testdata/benchmarks/windows-arm64-options.txt',
    [string]$OutputPath = 'singleflight/testdata/benchmarks/summary-options.json',
    [int]$ExpectedCases = 96,
    [int]$ExpectedSamples = 5
)

$ErrorActionPreference = 'Stop'
$invariant = [System.Globalization.CultureInfo]::InvariantCulture
$utf8 = New-Object System.Text.UTF8Encoding($false)
$lines = [System.IO.File]::ReadAllLines((Join-Path (Get-Location) $InputPath))
if (-not ($lines -contains 'PASS')) {
    throw 'Benchmark output does not contain PASS.'
}

function Median($values) {
    $sorted = @($values | Sort-Object)
    if ($sorted.Count -eq 0) { return $null }
    $middle = [int][math]::Floor($sorted.Count / 2)
    if ($sorted.Count % 2) { return $sorted[$middle] }
    return ($sorted[$middle - 1] + $sorted[$middle]) / 2
}

$rows = @()
foreach ($line in $lines) {
    if ($line -notmatch '^(Benchmark\S+)\s+\d+\s+([0-9.eE+-]+) ns/op(.*)$') { continue }
    $name = $matches[1]
    $ns = [double]::Parse($matches[2], $invariant)
    $rest = $matches[3]
    $bytes = $null; $allocs = $null; $backend = $null; $shared = $null
    if ($rest -match '([0-9.eE+-]+) B/op') { $bytes = [double]::Parse($matches[1], $invariant) }
    if ($rest -match '([0-9.eE+-]+) allocs/op') { $allocs = [double]::Parse($matches[1], $invariant) }
    if ($rest -match '([0-9.eE+-]+) backend/op') { $backend = [double]::Parse($matches[1], $invariant) }
    if ($rest -match '([0-9.eE+-]+) shared/op') { $shared = [double]::Parse($matches[1], $invariant) }
    if ($null -eq $bytes -or $null -eq $allocs) { throw "Missing allocation metrics: $name" }
    $rows += [pscustomobject]@{ Name=$name; Ns=$ns; Bytes=$bytes; Allocs=$allocs; Backend=$backend; Shared=$shared }
}

$groups = @($rows | Group-Object Name)
if ($groups.Count -ne $ExpectedCases) { throw "Expected $ExpectedCases cases, found $($groups.Count)." }
$summary = @()
foreach ($group in $groups) {
    if ($group.Count -ne $ExpectedSamples) { throw "Wrong sample count for $($group.Name): $($group.Count)" }
    $ns = @($group.Group.Ns | Sort-Object)
    $summary += [pscustomobject]@{
        Name = $group.Name
        Count = $group.Count
        MedianNs = Median $ns
        MinNs = $ns[0]
        MaxNs = $ns[-1]
        MedianBytes = Median $group.Group.Bytes
        MedianAllocs = Median $group.Group.Allocs
        MedianBackend = Median @($group.Group.Backend | Where-Object { $null -ne $_ })
        MedianShared = Median @($group.Group.Shared | Where-Object { $null -ne $_ })
    }
}
[System.IO.File]::WriteAllText((Join-Path (Get-Location) $OutputPath), ($summary | ConvertTo-Json -Depth 4), $utf8)
# Normalize PowerShell Tee-Object's UTF-16 output to version-control-friendly UTF-8.
[System.IO.File]::WriteAllLines((Join-Path (Get-Location) $InputPath), $lines, $utf8)
"Validated $($groups.Count) cases with $ExpectedSamples samples each."
