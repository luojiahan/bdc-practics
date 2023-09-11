package com.qingjiao.mr.weblog;

public class WebLogBean {
    private String remoteIP;
    private String remoteUser;
    private String requestTime;
    private String requestURL;
    private String status;
    private String sendBytes;
    private String referer;
    private String userAgent;

    // 标记：日志是否合法
    private boolean flag;

    public WebLogBean() {
    }


    // setter和getter方法
    public String getRemoteIP() {
        return remoteIP;
    }

    public void setRemoteIP(String remoteIP) {
        this.remoteIP = remoteIP;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }

    public String getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(String requestTime) {
        this.requestTime = requestTime;
    }

    public String getRequestURL() {
        return requestURL;
    }

    public void setRequestURL(String requestURL) {
        this.requestURL = requestURL;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getSendBytes() {
        return sendBytes;
    }

    public void setSendBytes(String sendBytes) {
        this.sendBytes = sendBytes;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    // toString

    @Override
    public String toString() {
        return "WebLogBean{" +
                "remoteIP='" + remoteIP + '\'' +
                ", remoteUser='" + remoteUser + '\'' +
                ", requestTime='" + requestTime + '\'' +
                ", requestURL='" + requestURL + '\'' +
                ", status='" + status + '\'' +
                ", sendBytes='" + sendBytes + '\'' +
                ", referer='" + referer + '\'' +
                ", userAgent='" + userAgent + '\'' +
                ", flag=" + flag +
                '}';
    }
}
