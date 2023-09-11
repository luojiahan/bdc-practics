package com.qingjiao.hdfs.test;

import com.qingjiao.hdfs.api.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;

public class APITest {
    @Test
    public void testGetFileSystem() throws IOException {
        FileSystem fs = HDFSUtils.getFileSystem();
        System.out.println(fs.getUri());
    }
}
