package com.qingjiao.mr.weblog;

import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.HashSet;
import java.util.Locale;

public class WebLogUtils {
    public static WebLogBean filterLog(String line) {
        WebLogBean webLogBean = new WebLogBean();
        String[] contents = line.split(" ");
        if (contents.length>11) {
            webLogBean.setRemoteIP(contents[0]);
            webLogBean.setRemoteUser(contents[1]);
            webLogBean.setRequestTime(dateTimeFormatter(contents[3].substring(1)));
            webLogBean.setRequestURL(contents[6]);
            webLogBean.setStatus(contents[8]);
            webLogBean.setSendBytes(contents[9]);
            webLogBean.setReferer(contents[10]);

            if (contents.length>12) {
                webLogBean.setUserAgent(contents[11]+" "+contents[12]);
            } else {
                webLogBean.setUserAgent(contents[11]);
            }

            // 根据状态码过滤
            if (Integer.parseInt(webLogBean.getStatus())>=400) {
                webLogBean.setFlag(false);
            } else {
                webLogBean.setFlag(true);
            }
        } else {
            webLogBean.setFlag(false);
        }

        return webLogBean;
    }

    // 过滤url
    public static WebLogBean filterByPvs(String line) {
        WebLogBean webLogBean = filterLog(line);
        HashSet<String> urls = new HashSet<>();
        urls.add("/about");
        urls.add("/black-ip-list/");
        urls.add("/cassandra-clustor/");
        urls.add("/finance-rhive-repurchase/");
        urls.add("/hadoop-family-roadmap/");
        urls.add("/hadoop-hive-intro/");
        urls.add("/hadoop-zookeeper-intro/");
        urls.add("/hadoop-mahout-roadmap/");

        // 判断当前日志中请求的url是否符合条件
        if(!urls.contains(webLogBean.getRequestURL())) {
            webLogBean.setFlag(false);
        }

        return webLogBean;
    }

    // 对日期时间进行格式化：18/Sep/2013:06:49:18 => 2013091806
    public static String dateTimeFormatter(String datetime) {
        // 定义原始的日期时间格式
        DateTimeFormatter sourceDateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        // 定义最终输出的日期时间格式
        DateTimeFormatter dstDateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");

        // 将字符串日期时间进行格式转换
        TemporalAccessor parse = sourceDateTimeFormatter.parse(datetime);

        // 对日期时间进行格式化
        String result = dstDateTimeFormatter.format(parse);

        return result;
    }

//    public static void main(String[] args) {
//        String test="18/Sep/2013:06:49:18";
//        String result = dateTimeFormatter(test);
//        System.out.println(result);
//    }
}
