package com.qgyun.hltgq.hltgqmq.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 闸门流量计算器（MQTT 四闸站实时流量）
 *
 * 依据 t_auto_hltgq_water_sluice_discharge 站点配置（version 最大记录）与
 * 实时水位/开度，判定闸门工况并计算站级总流量。判定流程：
 *
 *   第0步 h_g ≥ 孔高 height → 强制全开工况（闸门已开到底，避免 H 过大时 r 偏小误判）
 *   第1步 H ≤ 0（上游水位≤闸底）或 h_H < 0（下游水位低于闸底）→ 跳过
 *   第2步 r = h_g / H：r > 0.65 全开工况；r ≤ 0.65 有闸控制工况
 *   第3步 全开分支比较 h_H/H 与 0.7（自由/淹没）；有闸分支比较 h_1 与 h_g（自由/淹没）
 *
 * 四组公式（b=单孔净宽, g=9.81）：
 *   ① 全开自由流    Q = m·b·H·√(2gH)                    （过流高度取上游水深 H，自由流不受下游顶托）
 *   ② 全开淹没流    Q = φ·b·h_H·√(2g(H−h_H))
 *   ③ 有闸自由流    Q = μ·b·h_g·√(2g(H−0.65h_g))
 *   ④ 有闸淹没流    Q = μ'·b·h_g·√(2g·Z_1), Z_1 = H−h_1
 *
 * 边界归类（偏安全）：h_g=孔高 归全开；r=0.65 归有闸；h_H/H=0.7、h_1=h_g 归淹没流。
 * 多孔流量 = 单孔流量累加。守卫规则见 calculateSiteDischarge 注释。
 */
public final class GateDischargeCalculator {

    private static final Logger log = LoggerFactory.getLogger(GateDischargeCalculator.class);

    /** 重力加速度 (m/s²) */
    private static final double G = 9.81;

    /**
     * 单站计算流量上限(m³/s)：与遥测瞬时流量上限一致。
     * 超限说明输入异常（如水位越界、配置错误）导致计算失真，跳过流量入库。
     */
    private static final double MAX_SITE_DISCHARGE = 100;

    private GateDischargeCalculator() {
    }

    /**
     * 站点流量计算配置（sluice_discharge 表 version 最大的一条记录）
     */
    public static class SluiceConfig {
        public final String site;
        /** m：全开自由流流量系数 */
        public final double m;
        /** φ：全开淹没流流速系数 */
        public final double phi;
        /** μ：有闸自由流流量系数 */
        public final double mu;
        /** μ'：有闸淹没流修正流量系数 */
        public final double mu2;
        /** b：单孔净宽 (m) */
        public final double width;
        /** 闸底高程 (m) */
        public final double bottom;
        /** 孔高 (m)：全开判定阈值 */
        public final double height;

        public SluiceConfig(String site, double m, double phi, double mu, double mu2,
                            double width, double bottom, double height) {
            this.site = site;
            this.m = m;
            this.phi = phi;
            this.mu = mu;
            this.mu2 = mu2;
            this.width = width;
            this.bottom = bottom;
            this.height = height;
        }
    }

    /**
     * 计算站级总流量（Σ各孔单孔流量）。
     *
     * 返回 null 表示该站本次跳过，调用方跳过流量入库：
     *   - 上游水深 H ≤ 0（上游水位≤闸底）
     *   - 下游水深 h_H < 0（下游水位低于闸底，传感器异常/悬空；h_H=0 干涸属合法工况，不跳过）
     *   - 倒流（下游水位 ≥ 上游水位）
     *
     * 水位/开度缺失、无配置/脏配置的守卫由调用方负责（见 MqttGateDataService）。
     * 低水位差 / 超淹没度不跳过，照常计算并打 warn 日志标记（有效性甄别依赖日志）。
     *
     * @param upZ         上游水位 (m)
     * @param downZ       下游水位 (m)
     * @param openDegrees 各孔开度列表（长度 = 孔数）
     * @param siteId      站点 ID（仅用于日志）
     */
    public static Double calculateSiteDischarge(SluiceConfig cfg, double upZ, double downZ,
                                                List<Double> openDegrees, String siteId) {
        double H = upZ - cfg.bottom;
        if (H <= 0) {
            log.debug("上游水深≤0(水位低于闸底), 跳过流量计算: site={}, up_z={}, bottom={}",
                    siteId, upZ, cfg.bottom);
            return null;
        }

        double hH = downZ - cfg.bottom;
        // 下游水位低于闸底高程：物理异常（传感器悬空报0等），跳过流量计算。
        // h_H = 0（下游干涸）属合法工况，照常计算（有闸自由流分支天然处理）。
        if (hH < 0) {
            log.warn("下游水位异常(低于闸底), 跳过流量计算: site={}, down_z={}, bottom={}, h_H={}",
                    siteId, downZ, cfg.bottom, hH);
            return null;
        }
        if (downZ >= upZ) {
            log.debug("倒流(下游水位≥上游水位), 跳过流量计算: site={}, up_z={}, down_z={}",
                    siteId, upZ, downZ);
            return null;
        }

        // 低水位差：照常计算入库，warn 日志标记，下游使用方应谨慎引用
        if (H - hH <= 0.05) {
            log.warn("闸站流量低水位差(≤5cm), 照常计算入库, 数据仅供参考: site={}, H={}, h_H={}",
                    siteId, H, hH);
        }

        double total = 0;
        boolean warnedOverSubmerged = false;
        for (Double openDegree : openDegrees) {
            double hg = openDegree == null ? 0 : openDegree;

            // 第0步：闸门物理全开（开度达到孔高）→ 强制全开工况
            boolean fullOpen = hg >= cfg.height;
            // 第2步：开度比判别
            double r = fullOpen ? 1.0 : hg / H;

            double q;
            if (r > 0.65) {
                // ============ 全开工况 ============
                if (hH / H >= 0.7) {
                    // ② 全开淹没流
                    if (hH / H > 0.9 && !warnedOverSubmerged) {
                        log.warn("闸站流量超淹没度(h_H/H>0.9), 照常计算入库, 数据仅供参考: site={}, H={}, h_H={}",
                                siteId, H, hH);
                        warnedOverSubmerged = true;
                    }
                    q = cfg.phi * cfg.width * hH * Math.sqrt(2 * G * (H - hH));
                } else {
                    // ① 全开自由流：过流高度取上游水深 H
                    q = cfg.m * cfg.width * H * Math.sqrt(2 * G * H);
                }
            } else {
                // ============ 有闸控制工况 ============
                if (hH >= hg) {
                    // ④ 有闸淹没流（h_1 ≥ h_g，含等于）
                    if (hH / H > 0.9 && !warnedOverSubmerged) {
                        log.warn("闸站流量超淹没度(h_H/H>0.9), 照常计算入库, 数据仅供参考: site={}, H={}, h_H={}",
                                siteId, H, hH);
                        warnedOverSubmerged = true;
                    }
                    double z1 = H - hH;
                    q = cfg.mu2 * cfg.width * hg * Math.sqrt(2 * G * z1);
                } else {
                    // ③ 有闸自由流（r≤0.65 保证 H > 0.65h_g，√内天然为正）
                    q = cfg.mu * cfg.width * hg * Math.sqrt(2 * G * (H - 0.65 * hg));
                }
            }

            total += q;
        }

        // 输出守卫：NaN/∞(公式中间量异常)或超物理上限(输入水位/配置异常导致的失真) → 跳过流量入库
        if (Double.isNaN(total) || Double.isInfinite(total)) {
            log.warn("计算流量非有限值, 跳过流量入库: site={}, total={}", siteId, total);
            return null;
        }
        if (total > MAX_SITE_DISCHARGE) {
            log.warn("计算流量超上限({}m³/s), 疑似水位/配置异常, 跳过流量入库: site={}, up_z={}, total={}",
                    MAX_SITE_DISCHARGE, siteId, upZ, total);
            return null;
        }

        return total;
    }
}
