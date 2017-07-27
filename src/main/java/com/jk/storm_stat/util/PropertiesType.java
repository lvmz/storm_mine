package com.jk.storm_stat.util;

/**
 * Created by kouzhigang on 2015/12/2.
 */
public enum PropertiesType {
    STORM_STAT_ONLINE("druid_storm_stat_online.properties"),
    STORM_STAT_TEST("druid_storm_stat_test.properties");
    private String value;

    public String getValue() {
        return value;
    }

    PropertiesType(String value) {
        this.value = value;
    }
}
