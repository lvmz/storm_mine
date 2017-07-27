package com.jk.storm_stat.bean;

/**
 * Created by lou on 2017/5/16.
 */
public class ColumnsBean {

    public String name;
    public String type;
    public String desc;
    public boolean isnull;
    public String defaults;

    @Override
    public String toString() {
        return "ColumnsBean{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", desc='" + desc + '\'' +
                ", isnull=" + isnull +
                ", defaults='" + defaults + '\'' +
                '}';
    }
}
