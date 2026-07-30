$file = "E:\文件\启效\3.0\客户项目\中水三立-花凉亭\设备接入\数据接入\入库数据.txt"

# Find INSERT INTO lines
$insertLines = @()
$lineNum = 0
Get-Content $file -Encoding UTF8 | ForEach-Object {
    $lineNum++
    if ($_ -match "^INSERT INTO") {
        # Extract table name and first 300 chars
        $tableMatch = [regex]::Match($_, "INSERT INTO (\S+)")
        $tableName = if ($tableMatch.Success) { $tableMatch.Groups[1].Value } else { "UNKNOWN" }
        $insertLines += [PSCustomObject]@{Line=$lineNum; Table=$tableName; Sample=$_.Substring(0, [Math]::Min(300, $_.Length))}
    }
}

$insertLines | ForEach-Object {
    Write-Output "=== Line $($_.Line): $($_.Table) ==="
    Write-Output $_.Sample
    Write-Output ""
}
