package com.qgyun.hltgq.hltgqmq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 水位基准高程配置（stcd → 高程m）。
 * 入库水位 = 报文水位 + 基准高程（水深→海拔），仅配置的站点加高程。
 * 配置位于 application.properties 的 water-level-datum.datum.* 键下，
 * 新增/修改站点只需改配置，无需改代码。
 */
@Component
@ConfigurationProperties(prefix = "water-level-datum")
public class WaterLevelDatumProperties {

    /** stcd → 基准高程(m)，未配置的站点不加高程 */
    private Map<String, Double> datum = new LinkedHashMap<>();

    public Map<String, Double> getDatum() {
        return datum;
    }

    public void setDatum(Map<String, Double> datum) {
        this.datum = datum;
    }
}
