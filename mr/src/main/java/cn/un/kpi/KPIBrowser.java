package cn.un.kpi;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Iterator;

/**
 * 浏览器统计MapReduce
 *
 */
public class KPIBrowser {

    public static class KPIBrowserMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterBroswer(value.toString());
            if (kpi.isValid()) {

                word.set(kpi.getHttp_user_agent());
                output.collect(word, one);
            }
        }
    }

    public static class KPIBrowserReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // 自动快速地使用缺省Log4j环境
        long startTime = System.currentTimeMillis();
        BasicConfigurator.configure();
        String input = "hdfs://hadoop000:9000/input/journal.log";
        String output = "hdfs://hadoop000:9000/internetlogs/browser";

        // 配置
        JobConf conf = new JobConf(KPIBrowser.class);
        //	设置客户端访问datanode使用hostname来进行访问
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("fs.defaultFS", "hdfs://hadoop000:9000");
        System.setProperty("HADOOP_USER_NAME", "root");
        conf.setJobName("KPIBrowser");

        // 设置map输出的键值类型
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        // 设置Reduce输出kv的数据类型
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // 设置Map类、合并函数类、Reduce类
        conf.setMapperClass(KPIBrowserMapper.class);
        // 设置合并函数，该合并函数和reduce完成相同的功能，提升性能，减少map和reduce之间数据传输量
        conf.setCombinerClass(KPIBrowserReducer.class);
        conf.setReducerClass(KPIBrowserReducer.class);

        // 设置输入输出数据类型
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // 设置输入输出路径
        Path path = new Path(output);
        FileSystem fs= FileSystem.get(conf);
        if(fs.exists(path)){
            fs.delete(path,true);
        }
        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        // 启动任务
        JobClient.runJob(conf);
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        System.out.println("用时: " + executionTime / 1000  + "s");
        System.exit(0);
    }
}
