# Count INSERT statements per table
$file = "E:\文件\启效\3.0\客户项目\中水三立-花凉亭\设备接入\数据接入\入库数据.txt"
$lines = Get-Content $file
$insertCount = 0
$tableCounts = @{}

foreach ($line in $lines) {
    if ($line -match 'INSERT INTO\s+"?([^\s"(]+)"?') {
        $insertCount++
        $tableName = $Matches[1]
        if (-not $tableCounts.ContainsKey($tableName)) {
            $tableCounts[$tableName] = 0
        }
        $tableCounts[$tableName]++
    }
}

Write-Output "Total INSERT statements: $insertCount"
Write-Output "---"
foreach ($key in $tableCounts.Keys | Sort-Object) {
    Write-Output "$key : $($tableCounts[$key])"
}
