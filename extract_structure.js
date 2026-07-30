const fs = require('fs');

const filePath = 'E:\\文件\\启效\\3.0\\客户项目\\中水三立-花凉亭\\设备接入\\数据接入\\入库数据.txt';
const outPath = 'D:\\Java\\code\\hltgq\\hltgq-mq\\structure_out.txt';

const content = fs.readFileSync(filePath, 'utf-8');
const lines = content.split('\n');

const out = [];

lines.forEach((line, idx) => {
    const lnum = idx + 1;
    // Match table headers
    if (line.match(/数据：$/)) {
        out.push(`[${lnum}] HEADER: ${line.trim()}`);
        return;
    }
    // Match INSERT - extract table name only
    const m = line.match(/INSERT INTO\s+"?([^\s"(]+)"?/);
    if (m) {
        const tbl = m[1].replace(/^qixiao-apaas\./, '').replace(/^"qixiao-apaas"\./, '');
        // Get first 120 chars after VALUES
        const vIdx = line.indexOf('VALUES');
        const snippet = vIdx >= 0 ? line.substring(vIdx + 6, vIdx + 150).trim() : '';
        out.push(`[${lnum}] ${tbl} | ${snippet}`);
    }
});

fs.writeFileSync(outPath, out.join('\n'), 'utf-8');
console.log(`Done. ${out.length} entries written to ${outPath}`);
