# Generates bar charts for the latest PostgreSQL multi-threaded JMH run.
# Usage:
#   powershell -ExecutionPolicy Bypass -File benchmark/scripts/plot-jmh.ps1
#   powershell -ExecutionPolicy Bypass -File benchmark/scripts/plot-jmh.ps1 -Language en

param(
    [ValidateSet("zh", "en")]
    [string]$Language = "zh"
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Drawing

$benchmarkRoot = Split-Path -Parent $PSScriptRoot
$targetDir = Join-Path $benchmarkRoot "target"
$isEn = $Language -eq "en"
$chartDir = Join-Path $benchmarkRoot "charts"
if ($isEn) { $chartDir = Join-Path $chartDir "en" }
New-Item -ItemType Directory -Force -Path $chartDir | Out-Null

$text = if ($isEn) {
    @{
        PointThroughputTitle = "Point Query Throughput (PostgreSQL, 8 threads)"
        PointLatencyTitle = "Point Query Average Latency (PostgreSQL, 8 threads)"
        RangeThroughputTitle = "Range Query Throughput (10,000 rows, PostgreSQL, 8 threads)"
        RangeLatencyTitle = "Range Query Average Latency (10,000 rows, PostgreSQL, 8 threads)"
        ParserThroughputTitle = "SQL Parser Throughput (8 threads)"
        ParserLatencyTitle = "SQL Parser Average Latency (8 threads)"
        Higher = "8 threads; higher is better"
        Lower = "8 threads; lower is better"
        ThroughputAxis = "Throughput (ops/s)"
        LatencyUsAxis = "Average latency (µs/op)"
        LatencyMsAxis = "Average latency (ms/op)"
        PlainJdbc = "Plain JDBC"
    }
}
else {
    @{
        PointThroughputTitle = "随机主键点查吞吐（PostgreSQL，8 线程）"
        PointLatencyTitle = "随机主键点查平均延迟（PostgreSQL，8 线程）"
        RangeThroughputTitle = "随机范围查询吞吐（10,000 行，PostgreSQL，8 线程）"
        RangeLatencyTitle = "随机范围查询平均延迟（10,000 行，PostgreSQL，8 线程）"
        ParserThroughputTitle = "SQL 解析器吞吐（8 线程）"
        ParserLatencyTitle = "SQL 解析器平均延迟（8 线程）"
        Higher = "8 线程；越高越好"
        Lower = "8 线程；越低越好"
        ThroughputAxis = "吞吐量 (ops/s)"
        LatencyUsAxis = "平均延迟 (µs/op)"
        LatencyMsAxis = "平均延迟 (ms/op)"
        PlainJdbc = "纯 JDBC"
    }
}

$throughputPath = Join-Path $targetDir "jmh-result-pg-mt-thrpt.json"
$latencyPath = Join-Path $targetDir "jmh-result-pg-mt.json"

if (-not (Test-Path $throughputPath)) { throw "Missing $throughputPath" }
if (-not (Test-Path $latencyPath)) { throw "Missing $latencyPath" }

$throughputData = Get-Content $throughputPath -Raw | ConvertFrom-Json
$latencyData = Get-Content $latencyPath -Raw | ConvertFrom-Json

$throughput = @{}
foreach ($r in $throughputData) { $throughput[$r.benchmark] = [double]$r.primaryMetric.score }
$latency = @{}
foreach ($r in $latencyData) { $latency[$r.benchmark] = [double]$r.primaryMetric.score }

# Prefer dedicated parser runs when they exist.
$parserThroughputPath = Join-Path $targetDir "jmh-result-pg-mt-parser-thrpt.json"
$parserLatencyPath = Join-Path $targetDir "jmh-result-pg-mt-parser-latency.json"
$parserThroughputMap = $throughput.Clone()
$parserLatencyMap = $latency.Clone()
if (Test-Path $parserThroughputPath) {
    foreach ($r in (Get-Content $parserThroughputPath -Raw | ConvertFrom-Json)) {
        $parserThroughputMap[$r.benchmark] = [double]$r.primaryMetric.score
    }
}
if (Test-Path $parserLatencyPath) {
    foreach ($r in (Get-Content $parserLatencyPath -Raw | ConvertFrom-Json)) {
        $parserLatencyMap[$r.benchmark] = [double]$r.primaryMetric.score
    }
}

$palette = @{
    "plainJdbc"  = "#2C3E50"
    "sqala"      = "#E67E22"
    "hibernate"  = "#3498DB"
    "myBatis"    = "#16A085"
    "jooq"       = "#8E44AD"
    "slick"      = "#7F8C8D"
    "druid"      = "#2C3E50"
    "calcite"    = "#3498DB"
    "jsqlparser" = "#7F8C8D"
}

function Color-Brush([string]$hex) {
    New-Object System.Drawing.SolidBrush ([System.Drawing.ColorTranslator]::FromHtml($hex))
}

function Get-BenchmarkScore($table, [string]$benchmark, [string]$metric) {
    $full = "bench.$benchmark"
    if (-not $table.ContainsKey($full)) { throw "Missing benchmark $full ($metric)" }
    return [double]$table[$full]
}

function Draw-VerticalChart {
    param(
        [string]$Title,
        [string]$Subtitle,
        [string]$Note,
        [string[]]$Keys,
        [string[]]$Labels,
        [double[]]$Values,
        [string]$ValueFormat,
        [string]$AxisLabel,
        [double]$YMax,
        [double]$TickStep,
        [string]$TickFormat,
        [string]$OutputPath
    )

    $width = 1400
    $height = 800
    $bmp = New-Object System.Drawing.Bitmap $width, $height
    $g = [System.Drawing.Graphics]::FromImage($bmp)
    $g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $g.TextRenderingHint = [System.Drawing.Text.TextRenderingHint]::AntiAliasGridFit
    $g.Clear([System.Drawing.Color]::White)

    $titleFont = New-Object System.Drawing.Font "Microsoft YaHei", 26, ([System.Drawing.FontStyle]::Bold)
    $subtitleFont = New-Object System.Drawing.Font "Microsoft YaHei", 13, ([System.Drawing.FontStyle]::Regular)
    $noteFont = New-Object System.Drawing.Font "Microsoft YaHei", 12, ([System.Drawing.FontStyle]::Regular)
    $axisFont = New-Object System.Drawing.Font "Microsoft YaHei", 12, ([System.Drawing.FontStyle]::Regular)
    $labelFont = New-Object System.Drawing.Font "Microsoft YaHei", 14, ([System.Drawing.FontStyle]::Regular)
    $valueFont = New-Object System.Drawing.Font "Microsoft YaHei", 13, ([System.Drawing.FontStyle]::Bold)
    $dark = Color-Brush "#1F2937"
    $gray = Color-Brush "#6B7280"
    $axisPen = New-Object System.Drawing.Pen ([System.Drawing.Color]::FromArgb(55, 65, 81)), 2
    $gridPen = New-Object System.Drawing.Pen ([System.Drawing.Color]::FromArgb(229, 231, 235)), 1
    $gridPen.DashStyle = [System.Drawing.Drawing2D.DashStyle]::Dash

    $left = 130.0
    $top = 125.0
    $right = 60.0
    $bottom = 120.0
    $plotW = $width - $left - $right
    $plotH = $height - $top - $bottom

    $g.DrawString($Title, $titleFont, $dark, $left, 28.0)
    $g.DrawString($Subtitle, $subtitleFont, $gray, $left, 80.0)

    # Y grid and labels
    $ticks = [int][Math]::Round($YMax / $TickStep)
    for ($i = 0; $i -le $ticks; $i++) {
        $value = $i * $TickStep
        $y = $top + $plotH - ($value / $YMax) * $plotH
        $g.DrawLine($gridPen, [single]$left, [single]$y, [single]($left + $plotW), [single]$y)
        $tickText = & ([scriptblock]::Create($TickFormat)) $value
        $size = $g.MeasureString($tickText, $axisFont)
        $g.DrawString($tickText, $axisFont, $gray, [single]($left - $size.Width - 12), [single]($y - $size.Height / 2))
    }

    # Axes
    $g.DrawLine($axisPen, [single]$left, [single]$top, [single]$left, [single]($top + $plotH))
    $g.DrawLine($axisPen, [single]$left, [single]($top + $plotH), [single]($left + $plotW), [single]($top + $plotH))

    $slot = $plotW / $Keys.Count
    $barW = $slot * 0.58
    for ($i = 0; $i -lt $Keys.Count; $i++) {
        $barH = ($Values[$i] / $YMax) * $plotH
        $x = $left + $slot * $i + ($slot - $barW) / 2
        $y = $top + $plotH - $barH
        $brush = Color-Brush $palette[$Keys[$i]]
        $g.FillRectangle($brush, [single]$x, [single]$y, [single]$barW, [single]$barH)

        # Value label
        $valueText = & ([scriptblock]::Create($ValueFormat)) $Values[$i]
        $valueSize = $g.MeasureString($valueText, $valueFont)
        $valueX = $x + ($barW - $valueSize.Width) / 2
        $valueY = $y - $valueSize.Height - 6
        if ($valueY -lt ($top - 30)) { $valueY = $y + 8 }
        $g.DrawString($valueText, $valueFont, $dark, [single]$valueX, [single]$valueY)

        # Category label
        $labelText = $Labels[$i]
        $labelSize = $g.MeasureString($labelText, $labelFont)
        $labelX = $x + ($barW - $labelSize.Width) / 2
        $g.DrawString($labelText, $labelFont, $dark, [single]$labelX, [single]($top + $plotH + 18))
    }

    $g.DrawString($AxisLabel, $axisFont, $gray, [single]($left), [single]($height - 42))

    $bmp.Save($OutputPath, [System.Drawing.Imaging.ImageFormat]::Png)
    $g.Dispose()
    $bmp.Dispose()
    Write-Host "Wrote $OutputPath"
}

function Draw-HorizontalLinearChart {
    param(
        [string]$Title,
        [string]$Subtitle,
        [string]$Note,
        [string[]]$Keys,
        [string[]]$Labels,
        [double[]]$Values,
        [string]$ValueFormat,
        [string]$AxisLabel,
        [double]$AxisMax,
        [double]$TickStep,
        [string]$TickFormat,
        [string]$OutputPath
    )

    $width = 1500
    $height = 700
    $bmp = New-Object System.Drawing.Bitmap $width, $height
    $g = [System.Drawing.Graphics]::FromImage($bmp)
    $g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $g.TextRenderingHint = [System.Drawing.Text.TextRenderingHint]::AntiAliasGridFit
    $g.Clear([System.Drawing.Color]::White)

    $titleFont = New-Object System.Drawing.Font "Microsoft YaHei", 26, ([System.Drawing.FontStyle]::Bold)
    $subtitleFont = New-Object System.Drawing.Font "Microsoft YaHei", 13, ([System.Drawing.FontStyle]::Regular)
    $noteFont = New-Object System.Drawing.Font "Microsoft YaHei", 12, ([System.Drawing.FontStyle]::Regular)
    $axisFont = New-Object System.Drawing.Font "Microsoft YaHei", 12, ([System.Drawing.FontStyle]::Regular)
    $labelFont = New-Object System.Drawing.Font "Microsoft YaHei", 15, ([System.Drawing.FontStyle]::Regular)
    $valueFont = New-Object System.Drawing.Font "Microsoft YaHei", 14, ([System.Drawing.FontStyle]::Bold)
    $dark = Color-Brush "#1F2937"
    $gray = Color-Brush "#6B7280"
    $axisPen = New-Object System.Drawing.Pen ([System.Drawing.Color]::FromArgb(55, 65, 81)), 2
    $gridPen = New-Object System.Drawing.Pen ([System.Drawing.Color]::FromArgb(229, 231, 235)), 1
    $gridPen.DashStyle = [System.Drawing.Drawing2D.DashStyle]::Dash

    $left = 220.0
    $top = 125.0
    $right = 160.0
    $bottom = 95.0
    $plotW = $width - $left - $right
    $plotH = $height - $top - $bottom

    $g.DrawString($Title, $titleFont, $dark, $left, 28.0)
    $g.DrawString($Subtitle, $subtitleFont, $gray, $left, 80.0)

    $x = {
        param([double]$value)
        $left + ($value / $AxisMax) * $plotW
    }

    # Vertical grid lines
    $ticks = [int][Math]::Round($AxisMax / $TickStep)
    for ($i = 0; $i -le $ticks; $i++) {
        $value = $i * $TickStep
        $px = & $x $value
        $g.DrawLine($gridPen, [single]$px, [single]$top, [single]$px, [single]($top + $plotH))
        $tick = & ([scriptblock]::Create($TickFormat)) $value
        $size = $g.MeasureString($tick, $axisFont)
        $g.DrawString($tick, $axisFont, $gray, [single]($px - $size.Width / 2), [single]($top + $plotH + 14))
    }

    $g.DrawLine($axisPen, [single]$left, [single]$top, [single]$left, [single]($top + $plotH))
    $g.DrawLine($axisPen, [single]$left, [single]($top + $plotH), [single]($left + $plotW), [single]($top + $plotH))

    $slot = $plotH / $Keys.Count
    $barH = $slot * 0.56
    for ($i = 0; $i -lt $Keys.Count; $i++) {
        $y = $top + $slot * $i + ($slot - $barH) / 2
        $barEnd = & $x $Values[$i]
        $barW = [Math]::Max(2.0, $barEnd - $left)
        $brush = Color-Brush $palette[$Keys[$i]]
        $g.FillRectangle($brush, [single]$left, [single]$y, [single]$barW, [single]$barH)

        $labelText = $Labels[$i]
        $labelSize = $g.MeasureString($labelText, $labelFont)
        $g.DrawString($labelText, $labelFont, $dark, [single]($left - $labelSize.Width - 18), [single]($y + ($barH - $labelSize.Height) / 2))

        $valueText = & ([scriptblock]::Create($ValueFormat)) $Values[$i]
        $valueSize = $g.MeasureString($valueText, $valueFont)
        $valueX = $barEnd + 12
        if ($valueX + $valueSize.Width -gt ($width - 20)) {
            $valueX = $barEnd - $valueSize.Width - 12
        }
        $g.DrawString($valueText, $valueFont, $dark, [single]$valueX, [single]($y + ($barH - $valueSize.Height) / 2))
    }

    $g.DrawString($AxisLabel, $axisFont, $gray, [single]$left, [single]($height - 40))

    $bmp.Save($OutputPath, [System.Drawing.Imaging.ImageFormat]::Png)
    $g.Dispose()
    $bmp.Dispose()
    Write-Host "Wrote $OutputPath"
}

# ---------- Point query ----------
$pointKeys = @("plainJdbc", "sqala", "hibernate", "myBatis", "jooq", "slick")
$pointLabels = @($text.PlainJdbc, "sqala", "Hibernate", "MyBatis", "jOOQ", "Slick")
$pointThroughput = @()
$pointLatency = @()
foreach ($k in $pointKeys) {
    $pointThroughput += Get-BenchmarkScore $throughput "PointQueryBenchmark.$k" "throughput"
    $pointLatency += Get-BenchmarkScore $latency "PointQueryBenchmark.$k" "latency"
}

Draw-VerticalChart `
    -Title $text.PointThroughputTitle `
    -Subtitle $text.Higher `
    -Keys $pointKeys -Labels $pointLabels -Values $pointThroughput `
    -ValueFormat 'param($v) "{0:N0}" -f $v' `
    -AxisLabel $text.ThroughputAxis -YMax 220000 -TickStep 50000 `
    -TickFormat 'param($v) if ($v -ge 1000) { "{0:N0}k" -f ($v / 1000) } else { "0" }' `
    -OutputPath (Join-Path $chartDir "01-point-throughput.png")

Draw-VerticalChart `
    -Title $text.PointLatencyTitle `
    -Subtitle $text.Lower `
    -Keys $pointKeys -Labels $pointLabels -Values $pointLatency `
    -ValueFormat 'param($v) "{0:N2}" -f $v' `
    -AxisLabel $text.LatencyUsAxis -YMax 75 -TickStep 15 `
    -TickFormat 'param($v) "{0:N0}" -f $v' `
    -OutputPath (Join-Path $chartDir "02-point-latency.png")

# ---------- Range query ----------
$rangeKeys = @("plainJdbc", "sqala", "slick", "myBatis", "hibernate", "jooq")
$rangeLabels = @($text.PlainJdbc, "sqala", "Slick", "MyBatis", "Hibernate", "jOOQ")
$rangeThroughput = @()
$rangeLatency = @()
foreach ($k in $rangeKeys) {
    $rangeThroughput += Get-BenchmarkScore $throughput "RangeQueryBenchmark.$k" "throughput"
    $rangeLatency += Get-BenchmarkScore $latency "RangeQueryBenchmark.$k" "latency"
}

Draw-VerticalChart `
    -Title $text.RangeThroughputTitle `
    -Subtitle $text.Higher `
    -Keys $rangeKeys -Labels $rangeLabels -Values $rangeThroughput `
    -ValueFormat 'param($v) "{0:N0}" -f $v' `
    -AxisLabel $text.ThroughputAxis -YMax 1200 -TickStep 200 `
    -TickFormat 'param($v) "{0:N0}" -f $v' `
    -OutputPath (Join-Path $chartDir "03-range-throughput.png")

Draw-VerticalChart `
    -Title $text.RangeLatencyTitle `
    -Subtitle $text.Lower `
    -Keys $rangeKeys -Labels $rangeLabels -Values $rangeLatency `
    -ValueFormat 'param($v) "{0:N2}" -f $v' `
    -AxisLabel $text.LatencyMsAxis -YMax 20 -TickStep 4 `
    -TickFormat 'param($v) "{0:N0}" -f $v' `
    -OutputPath (Join-Path $chartDir "04-range-latency.png")

# ---------- Parser ----------
$parserKeys = @("druid", "sqala", "calcite", "jsqlparser")
$parserLabels = @("Alibaba Druid", "sqala", "Apache Calcite", "JSqlParser")
$parserThroughput = @()
$parserLatency = @()
foreach ($k in $parserKeys) {
    $parserThroughput += Get-BenchmarkScore $parserThroughputMap "ParserBenchmark.$k" "throughput"
    $parserLatency += Get-BenchmarkScore $parserLatencyMap "ParserBenchmark.$k" "latency"
}

Draw-HorizontalLinearChart `
    -Title $text.ParserThroughputTitle `
    -Subtitle $text.Higher `
    -Keys $parserKeys -Labels $parserLabels -Values $parserThroughput `
    -ValueFormat 'param($v) "{0:N0}" -f $v' `
    -AxisLabel $text.ThroughputAxis -AxisMax 2500000 -TickStep 500000 `
    -TickFormat 'param($v) if ($v -ge 1000000) { "{0:N1}M" -f ($v / 1000000) } else { "{0:N0}" -f $v }' `
    -OutputPath (Join-Path $chartDir "05-parser-throughput.png")

Draw-HorizontalLinearChart `
    -Title $text.ParserLatencyTitle `
    -Subtitle $text.Lower `
    -Keys $parserKeys -Labels $parserLabels -Values $parserLatency `
    -ValueFormat 'param($v) "{0:N2}" -f $v' `
    -AxisLabel $text.LatencyUsAxis -AxisMax 1200 -TickStep 200 `
    -TickFormat 'param($v) "{0:N0}" -f $v' `
    -OutputPath (Join-Path $chartDir "06-parser-latency.png")

Write-Host "Charts written to $chartDir"
