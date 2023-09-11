package com.qingjiao.staff;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Iterator;

/**
 * 用户每小时访问量统计
 */
public class WordCountTest {
	public static class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		Text k=new Text();
		IntWritable v=new IntWritable(1);


		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String[] words = value.toString().split(" ");

			for (String word : words) {
				k.set(word);

				// 写出
				output.collect(k,v);
			}
		}
	}

	public static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result=new IntWritable();
		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum=0;
			while (values.hasNext()) {
				sum+= values.next().get();
			}
			result.set(sum);

			output.collect(key,result);
		}
	}

	public static void main(String[] args) throws Exception {
		// 自动快速地使用缺省Log4j环境
		BasicConfigurator.configure();
		// 对应于HDFS中文件所在的位置路径
//        String input = "hdfs://hadoop000:9000/root/software/hadoop-2.7.7/README.txt";
//        String output = "hdfs://hadoop000:9000/root/wordcount/output2";
		String input = "file:/root/software/hadoop-2.7.7/README.txt";
		String output = "file:/root/wordcount/output2";


		// 配置
		JobConf conf = new JobConf(WordCountTest.class);
		//	设置客户端访问datanode使用hostname来进行访问
		conf.set("dfs.client.use.datanode.hostname", "true");
		conf.set("fs.defaultFS", "hdfs://hadoop000:9000");
		System.setProperty("HADOOP_USER_NAME", "root");
		conf.setJobName("KPITime");

		// 设置map输出的kv类型
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		// 设置Reduce输出kv的数据类型
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// 设置Map类、合并函数类和Reduce类
		conf.setMapperClass(WordCountMapper.class);
		// 设置合并函数，该合并函数和reduce完成相同的功能，提升性能，减少map和reduce之间数据传输量
		conf.setCombinerClass(WordCountReducer.class);
		conf.setReducerClass(WordCountReducer.class);

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

		// 启动任务
		JobClient.runJob(conf);
		System.exit(0);
	}
}