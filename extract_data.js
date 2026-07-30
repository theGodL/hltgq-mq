const fs = require('fs');
const path = require('path');

const filePath = 'E:\\文件\\启效\\3.0\\客户项目\\中水三立-花凉亭\\设备接入\\数据接入\\入库数据.txt';

const content = fs.readFileSync(filePath, 'utf-8');
const lines = content.split('\n');

const tableCounts = {};
const tableFirstLines = {};
const tableColumns = {};
let currentTable = '';

lines.forEach((line, idx) => {
    // Find table headers
    if (line.match(/数据：$/)) {
        currentTable = line.trim();
        return;
    }
    // Find INSERT statements
    const match = line.match(/INSERT INTO\s+"?([^\s"(]+)"?\s*\(([^)]+)\)/);
    if (match) {
        const tableName = match[1];
        const columns = match[2];
        
        if (!tableCounts[tableName]) {
            tableCounts[tableName] = 0;
            tableFirstLines[tableName] = idx + 1;
            // Store simplified table name and columns
            const shortName = tableName.replace(/^qixiao-apaas\./, '').replace(/^"qixiao-apaas"\./, '');
            tableColumns[shortName] = columns;
        }
        tableCounts[tableName]++;
    }
});

// Output summary
console.log('=== 数据文件结构摘要 ===');
console.log(`总行数: ${lines.length}`);
console.log('');

const sortedTables = Object.keys(tableFirstLines).sort((a, b) => tableFirstLines[a] - tableFirstLines[b]);

sortedTables.forEach(tableName => {
    const shortName = tableName.replace(/^qixiao-apaas\./, '').replace(/^"qixiao-apaas"\./, '');
    console.log(`表名: ${shortName}`);
    console.log(`  行号: ${tableFirstLines[tableName]}`);
    console.log(`  INSERT条数: ${tableCounts[tableName]}`);
    console.log(`  列: ${tableColumns[shortName] || '(未提取)'}`);
    console.log('');
});

console.log('=== 完成 ===');
