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
 * 统计每个资源路径的ip访问量
 */
public class KPIIP {

    public static class KPIIPMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        //private Text word = new Text();
        private Text ips = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterIPs(value.toString());
            if (kpi.isValid()) {
                /**
                 * 以ip为key
                 * 以1为value
                 */
                //word.set(kpi.getRequest());
                ips.set(kpi.getRemote_addr());
                output.collect(ips, new IntWritable(1));
            }
        }
    }

    public static class KPIIPReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        //private Set<String> count = new HashSet<String>();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            /**
             * 以ip为key
             * 对ip进行分组聚合统计总数values
             */
        	int sum=0;
            while (values.hasNext()) {
                sum+=values.next().get();
            }
            //结果集中存放的是独立ip的个数
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // 自动快速地使用缺省Log4j环境
        long startTime = System.currentTimeMillis();
    	BasicConfigurator.configure();
        // 对应于HDFS中文件所在的位置路径
        String input = "hdfs://hadoop000:9000/input/journal.log";
        String output = "hdfs://hadoop000:9000/internetlogs/ip";

        JobConf conf = new JobConf(KPIIP.class);
        // 设置客户端访问datanode使用hostname来进行访问
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("fs.defaultFS", "hdfs://hadoop000:9000");
        System.setProperty("HADOOP_USER_NAME", "root");
        conf.setJobName("KPIIP");

	    // 设置Map输出kv的数据类型
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

	    // 设置Reduce输出kv的数据类型
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

	    // 设置Map类、合并函数类和Reduce类
        conf.setMapperClass(KPIIPMapper.class);
	    // 设置合并函数，该合并函数和reduce完成相同的功能，提升性能，减少map和reduce之间数据传输量
        conf.setCombinerClass(KPIIPReducer.class);
        conf.setReducerClass(KPIIPReducer.class);

	    // 设置输入输出数据类型
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

	    // 设置输入输出路径
        Path path = new Path(output);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }
        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

	    // 启动程序
        JobClient.runJob(conf);
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        System.out.println("用时: " + executionTime / 1000  + "s");
        System.exit(0);
    }

}
