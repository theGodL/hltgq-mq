const fs = require('fs');

const filePath = 'E:\\文件\\启效\\3.0\\客户项目\\中水三立-花凉亭\\设备接入\\数据接入\\入库数据.txt';

const content = fs.readFileSync(filePath, 'utf-8');
const lines = content.split('\n');

console.log(`总行数: ${lines.length}`);

// Find all table boundaries
const tableStarts = [];
for (let i = 0; i < lines.length; i++) {
    const match = lines[i].match(/^(t_auto_hltgq_\S+?)数据[：:]/);
    if (match) {
        tableStarts.push({ lineNo: i, tableName: match[1] });
    }
}

console.log(`\n找到 ${tableStarts.length} 张表:\n`);
for (let idx = 0; idx < tableStarts.length; idx++) {
    const { lineNo, tableName } = tableStarts[idx];
    const startLine = lineNo + 2;
    const endLine = (idx + 1 < tableStarts.length) ? tableStarts[idx + 1].lineNo - 1 : lines.length - 1;
    console.log(`  表${idx+1}: ${tableName}`);
    console.log(`    起始行: ${lineNo+1}, 数据行范围: ${startLine+1}-${endLine+1}`);
    console.log(`    总行数(含空行): ${endLine - startLine + 1}`);
}

console.log('\n' + '='.repeat(80));
console.log('各表详细分析');
console.log('='.repeat(80));

for (let idx = 0; idx < tableStarts.length; idx++) {
    const { lineNo, tableName } = tableStarts[idx];
    const startLine = lineNo + 2;
    const endLine = (idx + 1 < tableStarts.length) ? tableStarts[idx + 1].lineNo - 1 : lines.length - 1;

    console.log(`\n${'='.repeat(60)}`);
    console.log(`表${idx+1}: ${tableName}`);
    console.log(`${'='.repeat(60)}`);

    // Find first INSERT to extract columns
    let cols = [];
    for (let i = lineNo; i < Math.min(lineNo + 5, lines.length); i++) {
        if (lines[i].trim().startsWith('INSERT INTO')) {
            const m = lines[i].match(/INSERT INTO .+? \((.+?)\) VALUES/);
            if (m) {
                cols = m[1].split(',').map(c => c.trim().replace(/"/g, ''));
                console.log(`  列数: ${cols.length}`);
                console.log(`  列名: ${cols.join(', ')}`);
            }
            break;
        }
    }

    // Count INSERT rows
    let insertCount = 0;
    let firstInsert = null;
    let lastInsert = null;
    for (let i = startLine; i <= Math.min(endLine, lines.length - 1); i++) {
        if (lines[i].trim().startsWith('INSERT INTO')) {
            insertCount++;
            if (firstInsert === null) firstInsert = { lineNo: i, content: lines[i] };
            lastInsert = { lineNo: i, content: lines[i] };
        }
    }
    console.log(`  INSERT语句数: ${insertCount}`);

    if (firstInsert) {
        let content = firstInsert.content.trim();
        if (content.length > 600) content = content.substring(0, 600) + '...[截断]';
        console.log(`  首行INSERT (行${firstInsert.lineNo+1}):`);
        console.log(`    ${content.substring(0, 500)}`);

        if (lastInsert && lastInsert.lineNo !== firstInsert.lineNo) {
            let lc = lastInsert.content.trim();
            if (lc.length > 600) lc = lc.substring(0, 600) + '...[截断]';
            console.log(`  末行INSERT (行${lastInsert.lineNo+1}):`);
            console.log(`    ${lc.substring(0, 500)}`);
        }
    }

    // Check non-INSERT lines
    const otherLines = [];
    for (let i = startLine; i <= Math.min(endLine, lines.length - 1); i++) {
        const s = lines[i].trim();
        if (s && !s.startsWith('INSERT INTO')) {
            otherLines.push({ lineNo: i, content: s.substring(0, 200) });
        }
    }
    if (otherLines.length > 0) {
        console.log(`  非INSERT行: ${otherLines.length}`);
        for (const ol of otherLines.slice(0, 5)) {
            console.log(`    行${ol.lineNo+1}: ${ol.content}`);
        }
    }
}

// Now do deeper analysis: extract key field values from each table
console.log('\n\n' + '='.repeat(80));
console.log('关键字段值分析');
console.log('='.repeat(80));

for (let idx = 0; idx < tableStarts.length; idx++) {
    const { lineNo, tableName } = tableStarts[idx];
    const startLine = lineNo + 2;
    const endLine = (idx + 1 < tableStarts.length) ? tableStarts[idx + 1].lineNo - 1 : lines.length - 1;

    // Extract first INSERT to get column order
    let cols = [];
    let firstInsertIdx = -1;
    for (let i = lineNo; i < Math.min(lineNo + 5, lines.length); i++) {
        if (lines[i].trim().startsWith('INSERT INTO')) {
            firstInsertIdx = i;
            const m = lines[i].match(/INSERT INTO .+? \((.+?)\) VALUES/);
            if (m) {
                cols = m[1].split(',').map(c => c.trim().replace(/"/g, ''));
            }
            break;
        }
    }

    // Extract all VALUES and map to columns
    const rows = [];
    for (let i = startLine; i <= Math.min(endLine, lines.length - 1); i++) {
        const line = lines[i].trim();
        if (line.startsWith('INSERT INTO')) {
            // Extract VALUES part
            const vm = line.match(/VALUES\s*\((.+)\);?\s*$/);
            if (vm) {
                // Simple split - careful with commas inside quotes
                const values = vm[1];
                const parsed = parseValues(values);
                if (parsed.length === cols.length) {
                    const row = {};
                    for (let j = 0; j < cols.length; j++) {
                        row[cols[j]] = parsed[j];
                    }
                    rows.push(row);
                }
            }
        }
    }

    console.log(`\n${tableName}: ${rows.length} 行数据`);

    if (rows.length === 0) continue;

    // Show first row key fields
    const firstRow = rows[0];
    const keyCols = ['id', 'iofhpi', 'zzkaec', 'site', 'device', 'stcd', 'stcm', 'name', 'ahieto', 'corp_code'];
    const keyVals = keyCols.filter(k => k in firstRow).map(k => `${k}=${firstRow[k]}`).join(', ');
    if (keyVals) {
        console.log(`  首行关键字段: ${keyVals}`);
    }

    // If multiple rows, also show last row
    if (rows.length > 1) {
        const lastRow = rows[rows.length - 1];
        const lkv = keyCols.filter(k => k in lastRow).map(k => `${k}=${lastRow[k]}`).join(', ');
        if (lkv) {
            console.log(`  末行关键字段: ${lkv}`);
        }
    }

    // Check unique IDs and ID format
    const ids = rows.map(r => r['id']).filter(Boolean);
    const uniqueIds = [...new Set(ids)];
    if (ids.length > 0) {
        console.log(`  ID字段: 总数=${ids.length}, 唯一=${uniqueIds.length}`);
        const sampleIds = uniqueIds.slice(0, 5);
        console.log(`  ID示例: ${sampleIds.join(', ')}`);
        // Check if IDs are UUID-like (32 hex chars) or short
        const uuidPattern = /^[0-9a-f]{32}$/;
        const uuidCount = ids.filter(id => uuidPattern.test(id)).length;
        const shortCount = ids.filter(id => id.length <= 25 && !uuidPattern.test(id)).length;
        const otherCount = ids.length - uuidCount - shortCount;
        console.log(`  ID格式: UUID(32位hex)=${uuidCount}, 短ID(<=25字符)=${shortCount}, 其他=${otherCount}`);
    }

    // Check unique device values
    const devices = rows.map(r => r['device']).filter(Boolean);
    if (devices.length > 0) {
        const uniqueDevices = [...new Set(devices)];
        console.log(`  device字段: 总数=${devices.length}, 唯一值=${uniqueDevices.length}`);
        console.log(`  device示例: ${uniqueDevices.slice(0, 5).join(', ')}`);
    }

    // Check NULL count for important columns
    for (const col of cols) {
        const nullCount = rows.filter(r => r[col] === 'NULL' || r[col] === '' || r[col] === undefined).length;
        if (nullCount > 0 && nullCount < rows.length) {
            // Only report partially NULL columns
        }
    }
}

console.log('\n全部分析完成!');

// Simple VALUES parser - handles quoted strings but not all edge cases
function parseValues(valuesStr) {
    const result = [];
    let current = '';
    let inQuote = false;
    for (let i = 0; i < valuesStr.length; i++) {
        const ch = valuesStr[i];
        if (ch === "'" && !inQuote) {
            inQuote = true;
            current += ch;
        } else if (ch === "'" && inQuote) {
            // Check for escaped quote ''
            if (i + 1 < valuesStr.length && valuesStr[i + 1] === "'") {
                current += "''";
                i++;
            } else {
                inQuote = false;
                current += ch;
            }
        } else if (ch === ',' && !inQuote) {
            result.push(current.trim());
            current = '';
        } else {
            current += ch;
        }
    }
    if (current.trim()) {
        result.push(current.trim());
    }
    return result;
}
