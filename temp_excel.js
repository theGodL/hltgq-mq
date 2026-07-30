const XLSX = require('./node_modules/xlsx');
const wb = XLSX.readFile('E:\\文件\\启效\\3.0\\客户项目\\中水三立-花凉亭\\设备接入\\数据接入\\花凉亭灌区转发标签.xls');

wb.SheetNames.forEach((name, i) => {
    const sh = wb.Sheets[name];
    const data = XLSX.utils.sheet_to_json(sh, {header:1, defval:''});
    console.log('========== Sheet ' + i + ': ' + name + ' (' + data.length + ' rows) ==========');
    
    // 打印前3行看表头
    console.log('--- Headers ---');
    data.slice(0, 3).forEach((r, j) => console.log('Row ' + (j+1) + ': ' + r.map(c => String(c||'').substring(0,25)).join(' | ')));
    
    // 搜索含 RX_020, RX_021, HMI_R 的行
    console.log('--- Search results ---');
    data.forEach((r, j) => {
        const rowStr = r.map(c => String(c||'')).join('|');
        if (rowStr.includes('RX_020') || rowStr.includes('RX_021') || rowStr.includes('HMI_R_') || rowStr.includes('HMI_RX_')) {
            console.log('Row ' + (j+1) + ': ' + r.map(c => String(c||'').substring(0,30)).join(' | '));
        }
    });
    console.log('');
});
