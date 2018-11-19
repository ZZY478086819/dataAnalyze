package com.zy.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zy.bean.webLogBean;
import com.zy.utils.WebLogParser;

/**
 * 
 * @author zy
 * step_1:处理原始日志，过滤出真实的PV请求：
 * 1.转换出时间格式，对缺少字段填充默认值
 */
public class WebLogPreProcess {
	public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration conf=new Configuration();
		try {
			Job job=Job.getInstance(conf);
			job.setJarByClass(WebLogPreProcess.class);
			job.setMapperClass(MyMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			Path input=new Path("/user/hadoop/in/");
			FileInputFormat.addInputPath(job, input);
			Path output=new Path("/user/hadoop/output/setup_1");
			if(output.getFileSystem(conf).exists(output)) {
				output.getFileSystem(conf).delete(output, true);
			}
			FileOutputFormat.setOutputPath(job, output);
			
			boolean isSuccess = job.waitForCompletion(true);
			System.exit(isSuccess?0:1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	private static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		Text mk =new Text();
		NullWritable mv=NullWritable.get();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			webLogBean bean=WebLogParser.parser(line);
			//清除无效字段
			if(bean!=null) {
				if(bean.isVaild()==false) return;
				mk.set(bean.toString());
				context.write(mk, mv);
			}
		}
	}
}
