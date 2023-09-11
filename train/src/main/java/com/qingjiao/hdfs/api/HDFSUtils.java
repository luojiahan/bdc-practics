package com.qingjiao.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HDFSUtils {
    public static FileSystem fs;
    static {
        try {
            fs=getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    // 获取文件对象
    public static FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop:9000");

        // 获取文件对象
        FileSystem fs = FileSystem.get(conf);

        return fs;
    }

    // 关闭文件对象
    public static void closeFileSystem() {
        if (null!=fs) {
            try {
                fs.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
