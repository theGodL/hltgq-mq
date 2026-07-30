const fs = require('fs');
const content = fs.readFileSync('D:/AI/数据/hltgq-mq.log', 'utf-8');
const lines = content.split('\n').filter(l => l.trim());

console.log('总行数:', lines.length);
console.log('');

const tagStats = {};
const nonNullFields = {};

lines.forEach(line => {
    const match = line.match(/收到 MonitorData 消息: (.+)$/);
    if (!match) return;
    try {
        const data = JSON.parse(match[1]);
        const tag = data.tag;
        const entity = data.entity;
        tagStats[tag] = (tagStats[tag] || 0) + 1;
        
        if (!nonNullFields[tag]) nonNullFields[tag] = {};
        
        for (const [key, value] of Object.entries(entity)) {
            if (!nonNullFields[tag][key]) {
                nonNullFields[tag][key] = { nullCount: 0, nonNullCount: 0, sampleValues: [] };
            }
            if (value === null || value === undefined) {
                nonNullFields[tag][key].nullCount++;
            } else {
                nonNullFields[tag][key].nonNullCount++;
                if (nonNullFields[tag][key].sampleValues.length < 5) {
                    nonNullFields[tag][key].sampleValues.push(value);
                }
            }
        }
    } catch(e) {}
});

console.log('=== 各类型消息数量 ===');
Object.entries(tagStats).sort(function(a,b) {return a[0].localeCompare(b[0])}).forEach(function(entry) {
    console.log('  ' + entry[0] + ': ' + entry[1]);
});

const keyFields = {
    'wtInfo': ['Z','Q','XSA','XSAVV','XSMXV','FLWCHRCD','WPTN','MSQMT','MSAMT','MSVMT','ITTP','FTF','BTF','TF','WP1','Z1','Z2'],
    'riverInfo': ['Z','Q','XSA','XSAVV','XSMXV','FLWCHRCD','Z1','Z2'],
    'volInfo': ['VOL','TMP','WC','GZM','SS','ITTP','VTA','VTB','VTC','VIA','VIB','VIC'],
    'rainInfo': ['DRP','PN05','INTV','PDR','DYP','WTH'],
    'nmIspInfo': ['DOX','CODMN','CODCR','BOD5','NH3N','CL','PH','COND'],
    'pcpInfo': ['AIRT','ATM','PH','COND','WT','DRP','Q','H']
};

console.log('');
console.log('=== 各类型中字段非null统计 ===');

Object.entries(keyFields).forEach(function(entry) {
    var tag = entry[0];
    var fields = entry[1];
    console.log('');
    console.log('--- ' + tag + ' (' + (tagStats[tag] || 0) + '条) ---');
    var stats = nonNullFields[tag] || {};
    fields.forEach(function(f) {
        var s = stats[f];
        if (s) {
            var total = s.nullCount + s.nonNullCount;
            var sample = s.sampleValues.length > 0 ? '  样例: ' + JSON.stringify(s.sampleValues.slice(0,3)) : '';
            if (s.nonNullCount === 0) {
                console.log('  ' + f + ': 全部null (0/' + total + ')');
            } else if (s.nullCount === 0) {
                console.log('  ' + f + ': 全部非null (' + total + '/' + total + ')' + sample);
            } else {
                console.log('  ' + f + ': null=' + s.nullCount + '  非null=' + s.nonNullCount + '  (总' + total + ')' + sample);
            }
        } else {
            console.log('  ' + f + ': 未出现');
        }
    });
});

// 额外: 列出所有测站
console.log('');
console.log('=== 出现的测站编号(STCD) ===');
var stcds = {};
lines.forEach(function(line) {
    var m = line.match(/"STCD":"([^"]+)"/);
    if (m) stcds[m[1]] = (stcds[m[1]] || 0) + 1;
});
Object.keys(stcds).forEach(function(k) {
    console.log('  ' + k + ': ' + stcds[k] + '次');
});

// 额外: 日期范围
console.log('');
console.log('=== 日期范围 ===');
var dates = [];
lines.forEach(function(line) {
    var m = line.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/);
    if (m) dates.push(m[0]);
});
if (dates.length > 0) {
    console.log('最早: ' + dates[0]);
    console.log('最晚: ' + dates[dates.length-1]);
}
