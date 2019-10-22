package com.xiaolin.CreteLog;

public class AccessLog {

    private String domain;  //用户账号，可能为null
    private String plat; //平台
    private String time;
    private String flow;
    private String ip;  // ==> 经纬度  省份/城市/运营商/构建标签/构建商圈

    public String getPlat() {
        return this.plat;
    }

    public void setPlat(final String plat) {
        this.plat = plat;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}

