const XLSX = require('./node_modules/xlsx');
const wb = XLSX.readFile('E:\\文件\\启效\\3.0\\客户项目\\中水三立-花凉亭\\设备接入\\数据接入\\花凉亭灌区转发标签.xls');

// 只看南山寺 Sheet
wb.SheetNames.forEach((name, i) => {
    if (!name.includes('南山寺') && !name.includes('进水闸')) return;
    const sh = wb.Sheets[name];
    const data = XLSX.utils.sheet_to_json(sh, {header:1, defval:''});
    console.log('=== Sheet '+i+': '+name+' ===');
    data.forEach((r, j) => {
        const rowStr = r.map(c => String(c||'')).join('|');
        if (rowStr.includes('HMI_R_') || rowStr.includes('HMI_RX_')) {
            // 列: 功能 | 变量描述 | 转发标签
            console.log((j+1) + ' | ' + String(r[1]||'').substring(0,10) + ' | ' + String(r[2]||'').substring(0,20) + ' | ' + String(r[8]||'').substring(0,30));
        }
    });
});
