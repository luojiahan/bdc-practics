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
 * 统计每个页面的访问次数
 * 每个访问网页的资源路径作为key，value记为1，
 */
public class KPIPV { 

    public static class KPIPVMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterPVs(value.toString());
            if (kpi.isValid()) {
                /**
                 * 以页面的资源路径为键，每访问一次，值为1
                 * 作为map任务的输出
                 */
                word.set(kpi.getRequest());
                output.collect(word, one);
            }
        }
    }

    public static class KPIPVReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
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
        BasicConfigurator.configure();
        // 对应于HDFS中文件所在的位置路径
        String input = "hdfs://hadoop000:9000/input/journal.log";
        String output ="hdfs://hadoop000:9000/internetlogs/pv";

        JobConf conf = new JobConf(KPIPV.class);
        //	设置客户端访问datanode使用hostname来进行访问
		conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("fs.defaultFS", "hdfs://hadoop000:9000");
        System.setProperty("HADOOP_USER_NAME", "root");
        conf.setJobName("KPIPV");

        // 设置map输出的键值类型
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        // 设置reduce输出的键值类型
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // 设置Map类、合并函数类和Reduce类
        conf.setMapperClass(KPIPVMapper.class);
        conf.setReducerClass(KPIPVReducer.class);
        // 设置合并函数，该合并函数和reduce完成相同的功能，提升性能，减少map和reduce之间数据传输量
        conf.setCombinerClass(KPIPVReducer.class);

        // 设置输入和输出文件的类型
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // 设置输入和输出文件的路径
        Path path = new Path(output);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }
        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        // 运行启动任务
        JobClient.runJob(conf);
        System.exit(0);
    }

}
