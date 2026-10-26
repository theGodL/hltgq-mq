-- 设备拆分上线：② 数据迁移（分批执行，均为 UPDATE，不删除任何行）
-- 生成：2026-10-26 15:01:01；依据只读核对 result29.out
--
-- !! 执行顺序 !! 
--   Part A（rain_adjust）必须与 mq 开关 app.device-split-enabled=true 的切换同一批次，
--   否则雨量补偿按 device 匹配会静默失效（花凉亭坝上）。
--   Part B（历史数据+type收敛）必须等开关打开并观察 1~2 个上报周期、
--   确认新数据已写入新设备之后再执行。
--   整脚本幂等：UPDATE 按旧设备 id 匹配，重复执行第二次影响 0 行（已完成部分不会重复搬）。

-- ==================== Part A：雨量补偿配置（与开关切换同批） ====================
BEGIN;

-- rain_adjust: 花凉亭坝上水位计# → 花凉亭坝上雨量计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_rain_adjust
   SET device = 'S1fhuZWcj0Y7XkR8R2xv', updated_at = now(), updated_by = 'SYSTEM'
 WHERE device = 'sY8rwpxtydfwxAnsFhBa';

COMMIT;

-- ==================== Part B：历史数据迁移（开关打开并观察确认后执行） ====================
BEGIN;

-- ---------- wt_nfo ----------
-- [wt_nfo] 1 行: 三联支渠进水闸1# → 三联支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'zPOfZxmqIGsW3bbtregC'
 WHERE device = 'cLuryFiOqvgCwMYkrgzm' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 下仓节制闸1# → 下仓节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'GuyJTdiJueY20eFvYOrh'
 WHERE device = 'DYzb07NdG5HtH6EDLvrg' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 639 行: 下前河雨量计# → 下前河水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'rc1K7YTevDsijI9KNT2c'
 WHERE device = 'JqwSFmWAj62FaHIWttXo';

-- ---------- wt_nfo ----------
-- [wt_nfo] 2 行: 东方红进水闸1# → 东方红进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'B20Tyc0HY08391pnmx1i'
 WHERE device = 'wlZKaI3R1YglngPeUxMG' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 中檀支渠进水闸1# → 中檀支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'r7Zx2rOrA0pUCjBbIy3r'
 WHERE device = '4W2U8tQjLfchhjpAaHgl' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 中沟支渠进水闸1# → 中沟支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 't5nCWQZlq5kj4irZ7ijV'
 WHERE device = 'PkmCRGv1hywcpf7eKd3z' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 乔木分干渠进水闸1# → 乔木分干渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'K6zSJ0FGU4SREFGOZiO0'
 WHERE device = 'vcE5Xuexaw1sz3NdeP06' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 678 行: 九田雨量计# → 九田水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'KAQvRagQeQTMcrFSdugG'
 WHERE device = 's0r6c5iBNrMVvLVn2qJY';

-- ---------- wt_nfo ----------
-- [wt_nfo] 3 行: 云集支渠进水闸1# → 云集支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'VRVZK6vpPcyYogWr7dG4'
 WHERE device = 'DNJVW9xzyh7y7KYyTdIw' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 1 行: 五圩支渠进水闸1# → 五圩支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'Zjo0GB12XOnclCU11CJn'
 WHERE device = 'JzRECbDaQnBqrugw0bNM' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 25 行: 余龙节制闸1# → 余龙节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'wPaGmKhuCbwgrmEUUELh'
 WHERE device = 'SCfY2Bgat5flcgp2LZTc' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 5,332 行: 北干渠进水闸1# → 北干渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'n9dWctzNFDPAOYArPlCg'
 WHERE device = 'MYMFQIuOwZDXNlzcC3QT' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 十里支渠进水闸1# → 十里支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'ywl2l8ndwsxkfS2L5JQD'
 WHERE device = 'n4dkJzg6O8kL4XpVHljD' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 南岭支渠进水闸1# → 南岭支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'g9kSnknK2WiBJ76Afazl'
 WHERE device = 'IyMF2NzdkNwO8QOrzRAZ' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 南川支渠进水闸1# → 南川支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'BfrXy0897RujT9M2ANV3'
 WHERE device = 'bwtM7LdI6hksGEnjdeKJ' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4,977 行: 南干渠进水闸1# → 南干渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'pXFUN6DMCMgZH57DovvU'
 WHERE device = 'TcAShqLLK4LdznXA3xKi' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 南畈支渠进水闸1# → 南畈支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'R94wSLVLvhIqYc2GGMoj'
 WHERE device = 'rORoyWKYFVS0mSgO95Cb' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 309 行: 双庙湖渡槽流量计# → 双庙湖渡槽水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'fj2w748fRb1DqE8o1UeJ'
 WHERE device = 'U3ReNoiudbIo8a3NIU91';

-- ---------- rain_info ----------
-- [rain_info] 4,874 行: 周家河水位计# → 周家河雨量计#（含软删行，不筛 deleted）
UPDATE "qixiao-apaas".t_auto_hltgq_water_rain_info
   SET device = 'KKU45JcEKcdRScifRm3U'
 WHERE device = 'guC7IBNEEJrnUv8UGa3';

-- ---------- wt_nfo ----------
-- [wt_nfo] 3 行: 团山支渠进水闸1# → 团山支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'FNB925lMuBTvrSDOcAgA'
 WHERE device = 'kDbp5TcIbo5veSsJUOFd' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 233 行: 夏家湖渡槽流量计# → 夏家湖渡槽水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'TSOAI3Csv8PQgmP3xJGz'
 WHERE device = 'CkiqsQAKhyOlgE7hu0cO';

-- ---------- wt_nfo ----------
-- [wt_nfo] 4 行: 大桥支渠进水闸1# → 大桥支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'YlgbcWRq58unwjHZkGjP'
 WHERE device = 'LjSFhIOvghGQRyeCmqEg' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4,490 行: 太宿干渠进水闸1# → 太宿干渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = '3aSjwagHiNsSXt3aLnKE'
 WHERE device = 'kQnPCB5nv7v0eI9O6INC' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 6,490 行: 太怀干渠进水闸1# → 太怀干渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = '0dZ4M5D58jSmuzxVSkpT'
 WHERE device = 'uBNicM0BRjS63C7nSwx4' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 2 行: 姚圩支渠进水闸1# → 姚圩支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'a8itefhSMOEGipUpQrgx'
 WHERE device = 'e9bD3EbaxKN3oietIxol' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 654 行: 姜家寨雨量计# → 姜家寨水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'MGp6VKLfc1HEewvF5L3U'
 WHERE device = 'rKtIYQgBOhZocM3n8Jaw';

-- [river_info] 695 行: 寺前雨量计# → 寺前水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'ryohzrcLFUtmVeyKAskw'
 WHERE device = 'kSp5RsyI8jvGh5qpOyu3';

-- ---------- wt_nfo ----------
-- [wt_nfo] 883 行: 建设支渠1# → 建设支渠流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'VZ4VQXSlDVslLuN30vph'
 WHERE device = 'xFXJS37kNysFXPoU1AQk' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 645 行: 弥陀雨量计# → 弥陀水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'l7Z6s8wZJEtquid0H368'
 WHERE device = 'gV1hX85dDIrG1JQHc5pf';

-- ---------- wt_nfo ----------
-- [wt_nfo] 4 行: 彭桥支渠进水闸1# → 彭桥支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'AB5HgqzRhLCq0dNlhoCj'
 WHERE device = '5hnjvMPl9CF7EzPmfMet' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 新坝支渠进水闸1# → 新坝支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'Otz4eOhEWahNLQy4z5bP'
 WHERE device = 'KgtG6al2HAt9pt7hmCzj' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- soil_data ----------
-- [soil_data] 14 行: 新坝支渠进水闸流量计# → 新坝支渠进水闸墒情探测#
UPDATE "qixiao-apaas".t_auto_hltgq_water_soil_data
   SET device = 'Hp9hhcKmigGm0Brhq1TO'
 WHERE device = 'Otz4eOhEWahNLQy4z5bP';

-- ---------- wt_nfo ----------
-- [wt_nfo] 792 行: 望牛墩节制闸1# → 望牛墩节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = '5Dg9BRQGmYY3Hwf6cHx9'
 WHERE device = '3NdSDAOz5ZVWqPFnYgiR' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 朝阳支渠进水闸1# → 朝阳支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'D04hMqizp6Y8uiqw7iTk'
 WHERE device = 'FKZ7i89mOhmEdtNaF8fH' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 1 行: 朱山咀进水闸1# → 朱山咀进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'Bt6hNhCOTWGy7pIg7Xy7'
 WHERE device = 'hp7HGD0UKPV0cSfOJzVT' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 杨林分干渠进水闸1# → 杨林分干渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'faDjrmVG9v6UGSubAHu8'
 WHERE device = 'yznsPcY4bDaXMJv5nuOV' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 杨林分干节制闸1# → 杨林分干节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = '0Ti2olFSKGMKXFXUf4lp'
 WHERE device = '8vHhjaX9dyNHrNneLgL4' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 杨树支渠进水闸1# → 杨树支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = '0EfJaim2eHfbSDoUWJ9z'
 WHERE device = '6ipVe4ZCgyzEsins0e0w' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 50 行: 杨辛2#直灌涵1# → 杨辛2#直灌涵流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'CKG4IQ1Q61CghOlVkGwW'
 WHERE device = 'sZj6LNBZC2z270Ps63VE' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 桥坊支渠进水闸1# → 桥坊支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'ncoIhJsHiyujFVEBvAZq'
 WHERE device = 'lnA2yNKsZ0dSLk8qIq0R' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 武昌支渠进水闸1# → 武昌支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'RvaiG23jPoZ0pBe8SAoC'
 WHERE device = '0jC6meRVnkhV6DNlIDLT' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 段垅泄洪闸1# → 段垅泄洪闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'nIltsWdeFOmkxjxCB94v'
 WHERE device = 'QLpxUdZp8loZSucz74Ag' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3,768 行: 段龙节制闸1# → 段龙节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'aEawLrHiELNO22D6UAF6'
 WHERE device = 'wrc2CKcSbBey7xF9JlJr' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4,416 行: 毕岭节制闸1# → 毕岭节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'mhDWj11mPHY8ldv84oGW'
 WHERE device = 'Ry3ro4h1T9DBL3y8yBST' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 永兴支渠进水闸1# → 永兴支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'QXVrxcl4hkxCymfDJiK2'
 WHERE device = '9Dy049CL3WgLxExj1Ptx' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 5,128 行: 汪元节制闸1# → 汪元节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'veClttW5qx0lINw25IEt'
 WHERE device = 'JFXAF9QHF2vNWt9LckN5' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 613 行: 河图铺雨量计# → 河图铺水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'htPg3LAODF2AB6JA1cCA'
 WHERE device = 'MWhphYu5qhN7oZGIy4ZI';

-- ---------- wt_nfo ----------
-- [wt_nfo] 3 行: 洋普支渠进水闸1# → 洋普支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'kzKO7TJvoOC53PloN3Jp'
 WHERE device = 'pTIqfPaoXmL4cBLW6q5w' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 26 行: 9999900001待接入# → 流量站点-1流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'tayv4k8HbVFyJtewx3w3'
 WHERE device = 'SX7vF8AdDN02a1h3H6F4' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 海棠洼节制闸1# → 海棠洼节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = '4ZrjRYeRg0wwICBJ1al5'
 WHERE device = 'TXRXAn7SXcKiB9DIU1CR' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 435 行: 滴露节制闸1# → 滴露节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'z7k4jEdfV7oEvkop67tK'
 WHERE device = 'GHjbvrV1E5zhAnZHTttw' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 父子岭分水闸1# → 父子岭分水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = '0M2yPU7UAizaZbKSHZdK'
 WHERE device = 'VXkx5fVZ2noJCKIwaOSO' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 父子岭渡槽分水闸1# → 父子岭渡槽分水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'rF4CD8f7UuEkdFQtBVln'
 WHERE device = 'pI1x4HE7pvW5gACw3Q9X' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 716 行: 牛镇雨量计# → 牛镇水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'QyVWCC61z6eTqnbvue1T'
 WHERE device = 'aenKjVkmouBS0Eu801ZZ';

-- ---------- wt_nfo ----------
-- [wt_nfo] 3 行: 王卓屋泄洪闸1# → 王卓屋泄洪闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'QXESZjd16UOmAgEjTNFX'
 WHERE device = 'Nf4HpkL3jz5mF0SC4p3t' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3,381 行: 王塘泄洪闸1# → 王塘泄洪闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'lpPxovKMVHARbN0FH5Xw'
 WHERE device = '3UewqLony5dEy5L9jJg4' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3,250 行: 王塘节制闸1# → 王塘节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'Lodbgmv12oLC1ohdX8L0'
 WHERE device = '55EP4mlflB5Dof4mtIhi' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 2,323 行: 王家桥节制闸1# → 王家桥节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'vxWwNT7wVs1cBWJCV9rL'
 WHERE device = 'cci8DBTwGXBpp0ANd1qD' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 574 行: 白帽雨量计# → 白帽水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'n0SXPEVRzbdXzOjM3hLg'
 WHERE device = 'Asb5vQA1uSgEjle7S2rE';

-- ---------- wt_nfo ----------
-- [wt_nfo] 4 行: 碎石支渠节制闸1# → 碎石支渠节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'BoTSNvnM6qOlKL3tZvdS'
 WHERE device = 'fUJVrOKCn6ThKPN14yIl' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 程岭支渠节制闸1# → 程岭支渠节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'MXMKDwMr54gncCFty6dx'
 WHERE device = 'TzadWifExMVmAlMy5wmN' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 稳石支渠进水闸1# → 稳石支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'xL8oQIdue2K2vx1gB5mx'
 WHERE device = '3pdU8UReWjxnn4ToiHtB' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 立新支渠进水闸1# → 立新支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'V4zlCIxqo7knPmswAlQS'
 WHERE device = 'xoZv0PLZP1HcAv4apfut' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 2 行: 红岭支渠进水闸1# → 红岭支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'subQIAUXRtP76aKibvAe'
 WHERE device = 'HbjUEr87GOUZcQXdFtmJ' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 红旗支渠进水闸1# → 红旗支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'iddSF96aH72H4UXg6I4M'
 WHERE device = 'JRPhlJk9166JuerbCrKZ' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 红星支渠节制闸1# → 红星支渠节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'FLfhxjivFuyBduDa0Rb4'
 WHERE device = 'x6KWnxrKVacZQNF7Qavm' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 联合支渠进水闸1# → 联合支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'FjCyU1kW0Z5qx9x8hHCW'
 WHERE device = '9UX5ebTAtsImbKYZOFgw' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 胜利支渠进水闸1# → 胜利支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'CAR1M16hkWgDHQxtN4C5'
 WHERE device = 'VAyjzRsY8zm6X4uK3fxi' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- rain_info ----------
-- [rain_info] 7,771 行: 花凉亭坝上水位计# → 花凉亭坝上雨量计#（含软删行，不筛 deleted）
UPDATE "qixiao-apaas".t_auto_hltgq_water_rain_info
   SET device = 'S1fhuZWcj0Y7XkR8R2xv'
 WHERE device = 'sY8rwpxtydfwxAnsFhBa';

-- [rain_info] 3,888 行: 花凉亭坝下水位计# → 花凉亭坝下雨量计#（含软删行，不筛 deleted）
UPDATE "qixiao-apaas".t_auto_hltgq_water_rain_info
   SET device = '1UPzEHImXzGQt9zQXTav'
 WHERE device = 'biWz3vtzOSJlnD5GUubG';

-- ---------- wt_nfo ----------
-- [wt_nfo] 912 行: 荞麦岭泄洪河1# → 荞麦岭泄洪河流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'No4IoVPPMFvuGfPvZ0c0'
 WHERE device = 'cPlXSihvKlc5fkhwpXDX' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 虎塘支渠进水闸1# → 虎塘支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = '98kSDWiXLOmPQc0lMjhi'
 WHERE device = 'VlAYIWDvTduD4iXt7S1e' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 11,351 行: 虎形潭渡槽流量计# → 虎形潭渡槽水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'x7UYsXbEIKW6Zpor3veJ'
 WHERE device = 'BJ8VPeQtqCK1cSgrArYE';

-- ---------- wt_nfo ----------
-- [wt_nfo] 3,089 行: 袁畈泄洪闸1# → 袁畈泄洪闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 's7Pr92zBl4CZ40w4i2fY'
 WHERE device = 'a1f7mxOYS5Yw8IY35qMA' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 360 行: 郝大屋泄洪闸1# → 郝大屋泄洪闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'VFs27YDpxu5wvZs36RO9'
 WHERE device = 'vUmI6RrqOIzMPT45wRKV' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3 行: 郭塘支渠进水闸1# → 郭塘支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'Xt19dvTrIRveBKnIZ9UW'
 WHERE device = 'lxN7RB4LvVd0JBxzZjCl' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 1,452 行: 长岭跌水节制闸1# → 长岭跌水节制闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = '4RjOvEsgkZlw1UtPfdhj'
 WHERE device = 'By7FWFI6i3cWtNhEOgl7' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 长马分干渠进水闸1# → 长马分干渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'Dxj2aeTFiAfN9nwnOyla'
 WHERE device = '7WhqfQ86PcRY52oAmlCC' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 3,211 行: 集岭电站进水闸1# → 集岭电站进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'RTo4NHkRAAD0l7csbLVT'
 WHERE device = 'z9l8FOWCbmWGYuZLA1uG' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 4 行: 马丰泄洪闸1# → 马丰泄洪闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'FaLQzHcbGm21CXvCfxnM'
 WHERE device = 'vG0Z9sLvN5AVZyU6pbYX' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 600 行: 马嘶铺雨量计# → 马嘶铺水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'xixeDAQWFcKqONiMLPqy'
 WHERE device = 'zm9XiobxV0V3DTzLDSwS';

-- ---------- wt_nfo ----------
-- [wt_nfo] 4 行: 鲁元支渠进水闸1# → 鲁元支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = '2QHgZC2lDpbjyAPeE2L7'
 WHERE device = 'T8XWZv8sUz1CNgdyGxVs' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- river_info ----------
-- [river_info] 633 行: 鲤鱼墩雨量计# → 鲤鱼墩水位计#
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
   SET device = 'yLCKLUJO6sMHfqfjZa2N'
 WHERE device = 'V8uN0oPxfH036lOywfhR';

-- ---------- wt_nfo ----------
-- [wt_nfo] 645 行: 雷湾直灌涵（左岸）流量计# → 黄花庙直灌涵流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'mS5BvnRRbH31M33awh2Q'
 WHERE device = 'Ae1FImPJCYlDT1IJqUlp' AND stcd IS NOT NULL AND stcd <> '';

-- [wt_nfo] 2 行: 龙王山支渠进水闸1# → 龙王山支渠进水闸流量计#（公式流量 stcd 空行保留原设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
   SET device = 'aMN2v8tIDdrfPtGXCm9l'
 WHERE device = 'kxxlolbB2kXcrqJ4636U' AND stcd IS NOT NULL AND stcd <> '';

-- ---------- 设备 type 收敛为单值（复用设备/闸孔设备） ----------
-- 五圩支渠进水闸流量计#: type #3#|#5# → #3#（保留流量计数据）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#3#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'Zjo0GB12XOnclCU11CJn' AND type IS DISTINCT FROM '#3#';

-- 北干渠进水闸1#: type null → #4#（闸孔设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#4#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'MYMFQIuOwZDXNlzcC3QT' AND type IS DISTINCT FROM '#4#';

-- 北干渠进水闸2#: type null → #4#（闸孔设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#4#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'bzpFqIcKyPPwXU5Zx3n3' AND type IS DISTINCT FROM '#4#';

-- 南干渠进水闸1#: type null → #4#（闸孔设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#4#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'TcAShqLLK4LdznXA3xKi' AND type IS DISTINCT FROM '#4#';

-- 南干渠进水闸2#: type null → #4#（闸孔设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#4#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'bMpR5Au2kMUJalnhxYdM' AND type IS DISTINCT FROM '#4#';

-- 夏家湖渡槽流量计#: type #3#|#5#|#6#|#1# → #3#（保留流量计数据）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#3#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'CkiqsQAKhyOlgE7hu0cO' AND type IS DISTINCT FROM '#3#';

-- 建设支渠1#: type null → #4#（闸孔设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#4#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'xFXJS37kNysFXPoU1AQk' AND type IS DISTINCT FROM '#4#';

-- 朱山咀进水闸流量计#: type #3#|#5# → #3#（保留流量计数据）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#3#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'Bt6hNhCOTWGy7pIg7Xy7' AND type IS DISTINCT FROM '#3#';

-- 汪元节制闸1#: type null → #4#（闸孔设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#4#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'JFXAF9QHF2vNWt9LckN5' AND type IS DISTINCT FROM '#4#';

-- 汪元节制闸2#: type null → #4#（闸孔设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#4#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'R17LnqG1vZ0xd73b808T' AND type IS DISTINCT FROM '#4#';

-- 汪元节制闸3#: type null → #4#（闸孔设备）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#4#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'l2AIaOytGaM4WWKhF5Rl' AND type IS DISTINCT FROM '#4#';

-- 猪娘塘斗渠流量计#: type #3#|#5# → #3#（保留流量计数据）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#3#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'zwVLMJ516rB1lVfOMgTy' AND type IS DISTINCT FROM '#3#';

-- 花凉亭坝上水位计#: type #1#|#2# → #1#（保留水位计数据）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#1#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE id = 'sY8rwpxtydfwxAnsFhBa' AND type IS DISTINCT FROM '#1#';

COMMIT;

-- ==================== 可选块：vol_info/msg_info 历史 device 置空 ====================
-- 新口径两表 device 留空（终端级报文不挂传感器）；历史行默认保留在旧设备上（不影响功能）。
-- 如需与新口径完全一致，取消注释执行（含旧设备全部 vol/msg 行）：
-- BEGIN;
-- UPDATE "qixiao-apaas".t_auto_hltgq_water_vol_info SET device = NULL WHERE device IN ('cLuryFiOqvgCwMYkrgzm', 'DYzb07NdG5HtH6EDLvrg', 'JqwSFmWAj62FaHIWttXo', 'wlZKaI3R1YglngPeUxMG', '4W2U8tQjLfchhjpAaHgl', 'PkmCRGv1hywcpf7eKd3z', 'vcE5Xuexaw1sz3NdeP06', 's0r6c5iBNrMVvLVn2qJY', 'DNJVW9xzyh7y7KYyTdIw', 'JzRECbDaQnBqrugw0bNM', 'SCfY2Bgat5flcgp2LZTc', 'MYMFQIuOwZDXNlzcC3QT', 'n4dkJzg6O8kL4XpVHljD', 'IyMF2NzdkNwO8QOrzRAZ', 'bwtM7LdI6hksGEnjdeKJ', 'TcAShqLLK4LdznXA3xKi', 'rORoyWKYFVS0mSgO95Cb', 'U3ReNoiudbIo8a3NIU91', 'guC7IBNEEJrnUv8UGa3', 'kDbp5TcIbo5veSsJUOFd', 'CkiqsQAKhyOlgE7hu0cO', 'LjSFhIOvghGQRyeCmqEg', 'kQnPCB5nv7v0eI9O6INC', 'uBNicM0BRjS63C7nSwx4', 'e9bD3EbaxKN3oietIxol', 'rKtIYQgBOhZocM3n8Jaw', 'kSp5RsyI8jvGh5qpOyu3', 'xFXJS37kNysFXPoU1AQk', 'gV1hX85dDIrG1JQHc5pf', '5hnjvMPl9CF7EzPmfMet', 'KgtG6al2HAt9pt7hmCzj', 'Otz4eOhEWahNLQy4z5bP', '3NdSDAOz5ZVWqPFnYgiR', 'FKZ7i89mOhmEdtNaF8fH', 'hp7HGD0UKPV0cSfOJzVT', 'yznsPcY4bDaXMJv5nuOV', '8vHhjaX9dyNHrNneLgL4', '6ipVe4ZCgyzEsins0e0w', 'sZj6LNBZC2z270Ps63VE', 'lnA2yNKsZ0dSLk8qIq0R', '0jC6meRVnkhV6DNlIDLT', 'QLpxUdZp8loZSucz74Ag', 'wrc2CKcSbBey7xF9JlJr', 'Ry3ro4h1T9DBL3y8yBST', '9Dy049CL3WgLxExj1Ptx', 'JFXAF9QHF2vNWt9LckN5', 'MWhphYu5qhN7oZGIy4ZI', 'pTIqfPaoXmL4cBLW6q5w', 'SX7vF8AdDN02a1h3H6F4', 'TXRXAn7SXcKiB9DIU1CR', 'GHjbvrV1E5zhAnZHTttw', 'VXkx5fVZ2noJCKIwaOSO', 'pI1x4HE7pvW5gACw3Q9X', 'aenKjVkmouBS0Eu801ZZ', 'Nf4HpkL3jz5mF0SC4p3t', '3UewqLony5dEy5L9jJg4', '55EP4mlflB5Dof4mtIhi', 'cci8DBTwGXBpp0ANd1qD', 'Asb5vQA1uSgEjle7S2rE', 'fUJVrOKCn6ThKPN14yIl', 'TzadWifExMVmAlMy5wmN', '3pdU8UReWjxnn4ToiHtB', 'xoZv0PLZP1HcAv4apfut', 'HbjUEr87GOUZcQXdFtmJ', 'JRPhlJk9166JuerbCrKZ', 'x6KWnxrKVacZQNF7Qavm', '9UX5ebTAtsImbKYZOFgw', 'VAyjzRsY8zm6X4uK3fxi', 'sY8rwpxtydfwxAnsFhBa', 'biWz3vtzOSJlnD5GUubG', 'cPlXSihvKlc5fkhwpXDX', 'VlAYIWDvTduD4iXt7S1e', 'BJ8VPeQtqCK1cSgrArYE', 'a1f7mxOYS5Yw8IY35qMA', 'vUmI6RrqOIzMPT45wRKV', 'lxN7RB4LvVd0JBxzZjCl', 'By7FWFI6i3cWtNhEOgl7', '7WhqfQ86PcRY52oAmlCC', 'z9l8FOWCbmWGYuZLA1uG', 'vG0Z9sLvN5AVZyU6pbYX', 'zm9XiobxV0V3DTzLDSwS', 'T8XWZv8sUz1CNgdyGxVs', 'V8uN0oPxfH036lOywfhR', 'Ae1FImPJCYlDT1IJqUlp', 'kxxlolbB2kXcrqJ4636U');
-- UPDATE "qixiao-apaas".t_auto_hltgq_water_msg_info SET device = NULL WHERE device IN ('cLuryFiOqvgCwMYkrgzm', 'DYzb07NdG5HtH6EDLvrg', 'JqwSFmWAj62FaHIWttXo', 'wlZKaI3R1YglngPeUxMG', '4W2U8tQjLfchhjpAaHgl', 'PkmCRGv1hywcpf7eKd3z', 'vcE5Xuexaw1sz3NdeP06', 's0r6c5iBNrMVvLVn2qJY', 'DNJVW9xzyh7y7KYyTdIw', 'JzRECbDaQnBqrugw0bNM', 'SCfY2Bgat5flcgp2LZTc', 'MYMFQIuOwZDXNlzcC3QT', 'n4dkJzg6O8kL4XpVHljD', 'IyMF2NzdkNwO8QOrzRAZ', 'bwtM7LdI6hksGEnjdeKJ', 'TcAShqLLK4LdznXA3xKi', 'rORoyWKYFVS0mSgO95Cb', 'U3ReNoiudbIo8a3NIU91', 'guC7IBNEEJrnUv8UGa3', 'kDbp5TcIbo5veSsJUOFd', 'CkiqsQAKhyOlgE7hu0cO', 'LjSFhIOvghGQRyeCmqEg', 'kQnPCB5nv7v0eI9O6INC', 'uBNicM0BRjS63C7nSwx4', 'e9bD3EbaxKN3oietIxol', 'rKtIYQgBOhZocM3n8Jaw', 'kSp5RsyI8jvGh5qpOyu3', 'xFXJS37kNysFXPoU1AQk', 'gV1hX85dDIrG1JQHc5pf', '5hnjvMPl9CF7EzPmfMet', 'KgtG6al2HAt9pt7hmCzj', 'Otz4eOhEWahNLQy4z5bP', '3NdSDAOz5ZVWqPFnYgiR', 'FKZ7i89mOhmEdtNaF8fH', 'hp7HGD0UKPV0cSfOJzVT', 'yznsPcY4bDaXMJv5nuOV', '8vHhjaX9dyNHrNneLgL4', '6ipVe4ZCgyzEsins0e0w', 'sZj6LNBZC2z270Ps63VE', 'lnA2yNKsZ0dSLk8qIq0R', '0jC6meRVnkhV6DNlIDLT', 'QLpxUdZp8loZSucz74Ag', 'wrc2CKcSbBey7xF9JlJr', 'Ry3ro4h1T9DBL3y8yBST', '9Dy049CL3WgLxExj1Ptx', 'JFXAF9QHF2vNWt9LckN5', 'MWhphYu5qhN7oZGIy4ZI', 'pTIqfPaoXmL4cBLW6q5w', 'SX7vF8AdDN02a1h3H6F4', 'TXRXAn7SXcKiB9DIU1CR', 'GHjbvrV1E5zhAnZHTttw', 'VXkx5fVZ2noJCKIwaOSO', 'pI1x4HE7pvW5gACw3Q9X', 'aenKjVkmouBS0Eu801ZZ', 'Nf4HpkL3jz5mF0SC4p3t', '3UewqLony5dEy5L9jJg4', '55EP4mlflB5Dof4mtIhi', 'cci8DBTwGXBpp0ANd1qD', 'Asb5vQA1uSgEjle7S2rE', 'fUJVrOKCn6ThKPN14yIl', 'TzadWifExMVmAlMy5wmN', '3pdU8UReWjxnn4ToiHtB', 'xoZv0PLZP1HcAv4apfut', 'HbjUEr87GOUZcQXdFtmJ', 'JRPhlJk9166JuerbCrKZ', 'x6KWnxrKVacZQNF7Qavm', '9UX5ebTAtsImbKYZOFgw', 'VAyjzRsY8zm6X4uK3fxi', 'sY8rwpxtydfwxAnsFhBa', 'biWz3vtzOSJlnD5GUubG', 'cPlXSihvKlc5fkhwpXDX', 'VlAYIWDvTduD4iXt7S1e', 'BJ8VPeQtqCK1cSgrArYE', 'a1f7mxOYS5Yw8IY35qMA', 'vUmI6RrqOIzMPT45wRKV', 'lxN7RB4LvVd0JBxzZjCl', 'By7FWFI6i3cWtNhEOgl7', '7WhqfQ86PcRY52oAmlCC', 'z9l8FOWCbmWGYuZLA1uG', 'vG0Z9sLvN5AVZyU6pbYX', 'zm9XiobxV0V3DTzLDSwS', 'T8XWZv8sUz1CNgdyGxVs', 'V8uN0oPxfH036lOywfhR', 'Ae1FImPJCYlDT1IJqUlp', 'kxxlolbB2kXcrqJ4636U');
-- COMMIT;
