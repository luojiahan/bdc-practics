package cn.un.kpi;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * @ClassName KPILogETL
 * @Description
 * @Author yiluohan1234
 * @Date 2022/10/18
 * @Version V1.0
 */
public class KPILogETL {
    public static class KPILogETLMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, NullWritable> {
        Text k = new Text();
        NullWritable v = NullWritable.get();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterETL(value.toString());

            k.set(kpi.toString());
            output.collect(k, v);
        }
    }

    public static void main(String[] args) throws Exception {
        // 自动快速地使用缺省Log4j环境
        BasicConfigurator.configure();
        // 对应于HDFS中文件所在的位置路径
        String input = "hdfs://hadoop000:9000/input/journal.log";
        String output = "hdfs://hadoop000:9000/clean";

        JobConf conf = new JobConf(KPILogETL.class);
        // 设置客户端访问datanode使用hostname来进行访问
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("fs.defaultFS", "hdfs://hadoop000:9000");
        System.setProperty("HADOOP_USER_NAME", "root");
        conf.setJobName("KPILogETL");

        // 设置Map类
        conf.setMapperClass(KPILogETLMapper.class);

        // 设置M输出kv的数据类型
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NullWritable.class);

        conf.setNumReduceTasks(0);

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
        System.exit(0);
    }
}
