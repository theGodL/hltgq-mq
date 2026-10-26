-- 设备拆分上线：① 预建传感器设备（INSERT，执行前先跑 03_verify.sql V0/V1 核对）
-- 生成：2026-10-26 15:01:01；依据只读核对 result29.out
-- 幂等护栏：WHERE NOT EXISTS(name)，重复执行不会重复插入
-- 说明：预建设备 status 显式写 '#2#'（离线），首笔报文由入库逻辑标回在线；未收数据前巡检保持离线属预期
BEGIN;

-- → 下前河水位计# (#1#)，承接: river_info 639 行(旧设备 下前河雨量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'rc1K7YTevDsijI9KNT2c', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '下前河水位计#', '0pEt58j2091oIIESXrd', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '下前河水位计#');

-- → 东方红进水闸流量计# (#3#)，承接: wt_nfo 2 行(旧设备 东方红进水闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'B20Tyc0HY08391pnmx1i', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '东方红进水闸流量计#', 'MthSite0000000000003', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '东方红进水闸流量计#');

-- → 九田水位计# (#1#)，承接: river_info 678 行(旧设备 九田雨量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'KAQvRagQeQTMcrFSdugG', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '九田水位计#', '0LIX1Zr88VfqwS2XxdB', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '九田水位计#');

-- → 北干渠进水闸流量计# (#3#)，承接: wt_nfo 5332 行(旧设备 北干渠进水闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'n9dWctzNFDPAOYArPlCg', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '北干渠进水闸流量计#', '4Ieu32Stnz8dTD8S8GD', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '北干渠进水闸流量计#');

-- → 南干渠进水闸流量计# (#3#)，承接: wt_nfo 4977 行(旧设备 南干渠进水闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'pXFUN6DMCMgZH57DovvU', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '南干渠进水闸流量计#', 'cOGko9nLfFz6rRAvKvY', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '南干渠进水闸流量计#');

-- → 双庙湖渡槽水位计# (#1#)，承接: river_info 309 行(旧设备 双庙湖渡槽流量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'fj2w748fRb1DqE8o1UeJ', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '双庙湖渡槽水位计#', 'aUu3C63QeBTqMWdoacx', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '双庙湖渡槽水位计#');

-- → 周家河雨量计# (#2#)，承接: rain_info 4874 行(旧设备 周家河水位计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'KKU45JcEKcdRScifRm3U', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '周家河雨量计#', '0puvPIlgT6E7Jg9OGpM', '#2#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '周家河雨量计#');

-- → 夏家湖渡槽水位计# (#1#)，承接: river_info 233 行(旧设备 夏家湖渡槽流量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'TSOAI3Csv8PQgmP3xJGz', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '夏家湖渡槽水位计#', 'AB7HSONgpojM10Q7OUm', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '夏家湖渡槽水位计#');

-- → 太宿干渠进水闸流量计# (#3#)，承接: wt_nfo 4490 行(旧设备 太宿干渠进水闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT '3aSjwagHiNsSXt3aLnKE', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '太宿干渠进水闸流量计#', 'k6V2otO7e4ZmEba7HMD', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '太宿干渠进水闸流量计#');

-- → 太怀干渠进水闸流量计# (#3#)，承接: wt_nfo 6490 行(旧设备 太怀干渠进水闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT '0dZ4M5D58jSmuzxVSkpT', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '太怀干渠进水闸流量计#', 'zxgbgrx0lUXS44Jklo8', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '太怀干渠进水闸流量计#');

-- → 姜家寨水位计# (#1#)，承接: river_info 654 行(旧设备 姜家寨雨量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'MGp6VKLfc1HEewvF5L3U', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '姜家寨水位计#', 'D5odmEA9yHHjF63y9uM', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '姜家寨水位计#');

-- → 寺前水位计# (#1#)，承接: river_info 695 行(旧设备 寺前雨量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'ryohzrcLFUtmVeyKAskw', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '寺前水位计#', 'IAZk1KKWQb35qwQwgNr', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '寺前水位计#');

-- → 弥陀水位计# (#1#)，承接: river_info 645 行(旧设备 弥陀雨量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'l7Z6s8wZJEtquid0H368', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '弥陀水位计#', 'MqElyOSacnvs03TbNV5', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '弥陀水位计#');

-- → 新坝支渠进水闸墒情探测# (#7#)，承接: soil_data 14 行(旧设备 新坝支渠进水闸流量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'Hp9hhcKmigGm0Brhq1TO', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '新坝支渠进水闸墒情探测#', 'ZcNonLfC56A7zmWBvzK', '#7#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '新坝支渠进水闸墒情探测#');

-- → 望牛墩节制闸流量计# (#3#)，承接: wt_nfo 792 行(旧设备 望牛墩节制闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT '5Dg9BRQGmYY3Hwf6cHx9', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '望牛墩节制闸流量计#', 'KqZvHx9KTMNRrOuxgYo', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '望牛墩节制闸流量计#');

-- → 段龙节制闸流量计# (#3#)，承接: wt_nfo 3768 行(旧设备 段龙节制闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'aEawLrHiELNO22D6UAF6', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '段龙节制闸流量计#', 'FuYQF5sUUsibX2QeNzD', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '段龙节制闸流量计#');

-- → 毕岭节制闸流量计# (#3#)，承接: wt_nfo 4416 行(旧设备 毕岭节制闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'mhDWj11mPHY8ldv84oGW', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '毕岭节制闸流量计#', 'SQcUlflXiUAC9eagJJx', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '毕岭节制闸流量计#');

-- → 汪元节制闸流量计# (#3#)，承接: wt_nfo 5128 行(旧设备 汪元节制闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'veClttW5qx0lINw25IEt', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '汪元节制闸流量计#', 'GwDmpPwcfw6sk8C9JtM', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '汪元节制闸流量计#');

-- → 河图铺水位计# (#1#)，承接: river_info 613 行(旧设备 河图铺雨量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'htPg3LAODF2AB6JA1cCA', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '河图铺水位计#', 'NvED8IXgypBp5njZPMz', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '河图铺水位计#');

-- → 滴露节制闸流量计# (#3#)，承接: wt_nfo 435 行(旧设备 滴露节制闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'z7k4jEdfV7oEvkop67tK', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '滴露节制闸流量计#', 'niMY92woCAPBR3UVGgn', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '滴露节制闸流量计#');

-- → 牛镇水位计# (#1#)，承接: river_info 716 行(旧设备 牛镇雨量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'QyVWCC61z6eTqnbvue1T', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '牛镇水位计#', '1kziVHqQEz5h53xOXLg', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '牛镇水位计#');

-- → 王塘泄洪闸流量计# (#3#)，承接: wt_nfo 3381 行(旧设备 王塘泄洪闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'lpPxovKMVHARbN0FH5Xw', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '王塘泄洪闸流量计#', 'yx3ZEhC1nSN3io5RJCc', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '王塘泄洪闸流量计#');

-- → 王塘节制闸流量计# (#3#)，承接: wt_nfo 3250 行(旧设备 王塘节制闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'Lodbgmv12oLC1ohdX8L0', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '王塘节制闸流量计#', 'Q2HRjGaOE7RWihGJagS', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '王塘节制闸流量计#');

-- → 王家桥节制闸流量计# (#3#)，承接: wt_nfo 2323 行(旧设备 王家桥节制闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'vxWwNT7wVs1cBWJCV9rL', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '王家桥节制闸流量计#', 'LCmIfy4WB6lQ3OROQRo', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '王家桥节制闸流量计#');

-- → 白帽水位计# (#1#)，承接: river_info 574 行(旧设备 白帽雨量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'n0SXPEVRzbdXzOjM3hLg', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '白帽水位计#', 'OyYfqlbNlRlzqeYaMzf', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '白帽水位计#');

-- → 花凉亭坝上雨量计# (#2#)，承接: rain_info 7771 行(旧设备 花凉亭坝上水位计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'S1fhuZWcj0Y7XkR8R2xv', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '花凉亭坝上雨量计#', 'heLqnpaEBfGyLQauYcA', '#2#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '花凉亭坝上雨量计#');

-- → 花凉亭坝下雨量计# (#2#)，承接: rain_info 3888 行(旧设备 花凉亭坝下水位计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT '1UPzEHImXzGQt9zQXTav', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '花凉亭坝下雨量计#', '522gqVV6Vvi9TnxQjhY', '#2#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '花凉亭坝下雨量计#');

-- → 荞麦岭泄洪河流量计# (#3#)，承接: wt_nfo 912 行(旧设备 荞麦岭泄洪河1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'No4IoVPPMFvuGfPvZ0c0', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '荞麦岭泄洪河流量计#', 'NJqv7sNjkTce3VeTXjk', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '荞麦岭泄洪河流量计#');

-- → 虎形潭渡槽水位计# (#1#)，承接: river_info 11351 行(旧设备 虎形潭渡槽流量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'x7UYsXbEIKW6Zpor3veJ', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '虎形潭渡槽水位计#', 'NvVn4HJbO6RE8IhV0kv', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '虎形潭渡槽水位计#');

-- → 袁畈泄洪闸流量计# (#3#)，承接: wt_nfo 3089 行(旧设备 袁畈泄洪闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 's7Pr92zBl4CZ40w4i2fY', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '袁畈泄洪闸流量计#', 'dcWsH6rGjniFOxg4OR3', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '袁畈泄洪闸流量计#');

-- → 郝大屋泄洪闸流量计# (#3#)，承接: wt_nfo 360 行(旧设备 郝大屋泄洪闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'VFs27YDpxu5wvZs36RO9', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '郝大屋泄洪闸流量计#', 'PiTASJ0oNB5OHLQTvPd', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '郝大屋泄洪闸流量计#');

-- → 长岭跌水节制闸流量计# (#3#)，承接: wt_nfo 1452 行(旧设备 长岭跌水节制闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT '4RjOvEsgkZlw1UtPfdhj', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '长岭跌水节制闸流量计#', 'pJgk6PcRYVqd9kgYsdl', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '长岭跌水节制闸流量计#');

-- → 集岭电站进水闸流量计# (#3#)，承接: wt_nfo 3211 行(旧设备 集岭电站进水闸1#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'RTo4NHkRAAD0l7csbLVT', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '集岭电站进水闸流量计#', 'VzoiGl9W3XT7WheoEXV', '#3#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '集岭电站进水闸流量计#');

-- → 马嘶铺水位计# (#1#)，承接: river_info 600 行(旧设备 马嘶铺雨量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'xixeDAQWFcKqONiMLPqy', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '马嘶铺水位计#', 'MQvJMB2ePc8jTBURC7f', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '马嘶铺水位计#');

-- → 鲤鱼墩水位计# (#1#)，承接: river_info 633 行(旧设备 鲤鱼墩雨量计#)
INSERT INTO "qixiao-apaas".t_auto_hltgq_water_device (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type, status)
SELECT 'yLCKLUJO6sMHfqfjZa2N', 'hltgq', now(), 'SYSTEM', now(), 'SYSTEM', '鲤鱼墩水位计#', 'kYZKJXGhxesNXaReQWN', '#1#', '#2#'
 WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE name = '鲤鱼墩水位计#');

COMMIT;
