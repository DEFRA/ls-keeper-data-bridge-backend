# Coverage of the lines this PR adds, from the same XML the Sonar scanner consumes.
param(
    [string]$CoverageXml = 'coverage.xml',
    [string]$BaseRef = 'develop'
)

# Which lines the PR adds, per file, straight from the diff.
$added = @{}
$current = $null

git diff --unified=0 "$BaseRef...HEAD" -- 'src/**/*.cs' | ForEach-Object {
    if ($_ -match '^\+\+\+ b/(.+)$') {
        $current = $Matches[1]
        $added[$current] = New-Object 'System.Collections.Generic.HashSet[int]'
    }
    elseif ($current -and $_ -match '^@@ -\S+ \+(\d+)(?:,(\d+))? @@') {
        $start = [int]$Matches[1]
        $count = if ($Matches[2]) { [int]$Matches[2] } else { 1 }
        for ($i = 0; $i -lt $count; $i++) { [void]$added[$current].Add($start + $i) }
    }
}

# Which of those lines the tests executed.
$xml = [xml](Get-Content $CoverageXml)
$covered = @{}
$known = @{}
$root = ((Get-Location).Path -replace '\\', '/').TrimEnd('/') + '/'

foreach ($module in $xml.results.modules.module) {
    $paths = @{}
    foreach ($sf in $module.source_files.source_file) { $paths[$sf.id] = $sf.path }

    foreach ($fn in $module.functions.function) {
        foreach ($range in $fn.ranges.range) {
            $path = $paths[$range.source_id]
            if (-not $path) { continue }

            $rel = ($path -replace '\\', '/')
            if (-not $rel.StartsWith($root, 'OrdinalIgnoreCase')) { continue }
            $rel = $rel.Substring($root.Length)
            if (-not $rel.StartsWith('src/')) { continue }

            if (-not $known.ContainsKey($rel)) { $known[$rel] = New-Object 'System.Collections.Generic.HashSet[int]' }
            if (-not $covered.ContainsKey($rel)) { $covered[$rel] = New-Object 'System.Collections.Generic.HashSet[int]' }

            for ($line = [int]$range.start_line; $line -le [int]$range.end_line; $line++) {
                [void]$known[$rel].Add($line)
                if ($range.covered -eq 'yes') { [void]$covered[$rel].Add($line) }
            }
        }
    }
}

$rows = foreach ($file in $added.Keys) {
    $project = ($file -split '/')[1]
    $executable = if ($known.ContainsKey($file)) { @($added[$file] | Where-Object { $known[$file].Contains($_) }) } else { @() }
    $hit = if ($covered.ContainsKey($file)) { @($executable | Where-Object { $covered[$file].Contains($_) }) } else { @() }

    [PSCustomObject]@{
        Project    = $project
        File       = $file
        Executable = $executable.Count
        Covered    = $hit.Count
    }
}

"Per project:"
$rows | Group-Object Project | ForEach-Object {
    $e = ($_.Group | Measure-Object Executable -Sum).Sum
    $c = ($_.Group | Measure-Object Covered -Sum).Sum
    [PSCustomObject]@{
        Project    = $_.Name
        NewLines   = $e
        Covered    = $c
        Percent    = if ($e -gt 0) { [math]::Round(100 * $c / $e, 1) } else { 0 }
    }
} | Sort-Object NewLines -Descending | Format-Table -AutoSize

$allE = ($rows | Measure-Object Executable -Sum).Sum
$allC = ($rows | Measure-Object Covered -Sum).Sum
$noToolRows = $rows | Where-Object { $_.Project -ne 'KeeperData.Etl.Tool' }
$ntE = ($noToolRows | Measure-Object Executable -Sum).Sum
$ntC = ($noToolRows | Measure-Object Covered -Sum).Sum

"Including the tool : {0}/{1} = {2}%" -f $allC, $allE, [math]::Round(100 * $allC / $allE, 1)
"Excluding the tool : {0}/{1} = {2}%" -f $ntC, $ntE, [math]::Round(100 * $ntC / $ntE, 1)

"`nUncovered new lines outside the tool, worst first:"
$noToolRows | Where-Object { $_.Executable -gt $_.Covered } |
    Select-Object File, Executable, Covered, @{n = 'Missing'; e = { $_.Executable - $_.Covered } } |
    Sort-Object Missing -Descending | Select-Object -First 15 | Format-Table -AutoSize
