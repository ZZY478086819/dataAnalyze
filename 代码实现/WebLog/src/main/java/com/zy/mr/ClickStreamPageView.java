package com.zy.mr;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zy.bean.webLogBean;

/**
 * 
 * @author zy 1.将从access_log经过step_1清洗出来的数据梳理出点击流pageviews模型数据
 *         (1)区分出每次会话：sessionID如果上一次的时间和本次时间的时间间隔，超过30分钟，则认为是一次会话
 *         (2)给每一次visit（session）增加了session-id（随机uuid）
 */
public class ClickStreamPageView {
	public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration conf = new Configuration();
		try {
			Job job=Job.getInstance(conf);
			job.setJarByClass(ClickStreamPageView.class);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(webLogBean.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			Path inpath=new Path("/user/hadoop/output/setup_1");
			FileInputFormat.addInputPath(job, inpath);
			Path output=new Path("/user/hadoop/output/setup_2");
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

	private static class MyMapper extends Mapper<LongWritable, Text, Text, webLogBean> {
		Text mk = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, webLogBean>.Context context)
				throws IOException, InterruptedException {
			String line[] = value.toString().split("\001");
			if (line.length < 9)
				return;
			webLogBean bean = new webLogBean(line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8]);
			mk.set(bean.getIp_addr());
			context.write(mk, bean);
		}
	}

	private static class MyReducer extends Reducer<Text, webLogBean, NullWritable, Text> {
		NullWritable rk = NullWritable.get();
		Text rv = new Text();

		@Override
		protected void reduce(Text key, Iterable<webLogBean> values,
				Reducer<Text, webLogBean, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// 用于存储iP相同的一组的记录数
			ArrayList<webLogBean> beans = new ArrayList<webLogBean>();
			for (webLogBean b : values) {
				webLogBean bean = new webLogBean();
				try {
					BeanUtils.copyProperties(bean, b);
					beans.add(bean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
			//按时间先后，将beans中的记录进行排序
			Collections.sort(beans, new Comparator<webLogBean>() {
				@Override
				public int compare(webLogBean o1, webLogBean o2) {
					Date d1 = toDate(o1.getTime());
					Date d2 = toDate(o2.getTime());
					if(d1==null||d2==null) {
						return 0;
					}
					return d1.compareTo(d2);
				}
			});
			/**
			 * 从有序的beans中分辨出个次的visit,并对一次visit中的所访问的page按顺序标号step
			 * 核心思想：
			 * 就是比较相邻两条记录中的时间差，如果时间差<30分钟，则该两条记录属于同一个session，否则就是不同的session
			 *默认
			 */
			//用于表示page的顺序
			int step=1;
			String sessionID=UUID.randomUUID().toString();
			for(int i=0;i<beans.size();i++) {
				webLogBean b1=beans.get(i);
				//如果只有一条数据，那么直接输出
				if(beans.size()==1) {
					rv.set(sessionID+
							"\001"+key.toString()+
							"\001"+b1.getUser()+
							"\001"+b1.getTime()+
							"\001"+b1.getRequest()+
							"\001"+step+
							"\001"+(60)+
							"\001"+b1.getHttp_referer()+
							"\001"+b1.getUser_agent()+
							"\001"+b1.getBody_bytes_sent()+
							"\001"+b1.getStatus_code());
					sessionID=UUID.randomUUID().toString();
					context.write(rk, rv);
					break;
				}
				//由于需要两条数据来确定时间，所有放弃第一次
				if(i==0) {
					continue;
				}
				// 求近两次时间差
				long timediff=timeDiff(toDate(b1.getTime()),toDate(beans.get(i-1).getTime()));
				// 如果本次-上次时间差<30分钟，则输出前一次的页面访问信息
				if(timediff<1000*60*30) {
					rv.set(sessionID+
							"\001"+key.toString()+
							"\001"+b1.getUser()+
							"\001"+b1.getTime()+
							"\001"+b1.getRequest()+
							"\001"+step+
							"\001"+(timediff / 1000)+
							"\001"+b1.getHttp_referer()+
							"\001"+b1.getUser_agent()+
							"\001"+b1.getBody_bytes_sent()+
							"\001"+b1.getStatus_code());
					context.write(rk, rv);
					step++;
				}else {
					// 如果本次-上次时间差>30分钟，则输出前一次的页面访问信息且将step重置，以分隔为新的visit
					rv.set(sessionID+
							"\001"+key.toString()+
							"\001"+b1.getUser()+
							"\001"+b1.getTime()+
							"\001"+b1.getRequest()+
							"\001"+step+
							"\001"+(60)+
							"\001"+b1.getHttp_referer()+
							"\001"+b1.getUser_agent()+
							"\001"+b1.getBody_bytes_sent()+
							"\001"+b1.getStatus_code());
					context.write(rk, rv);
					sessionID=UUID.randomUUID().toString();
					// 输出完上一条之后，重置step编号
					step = 1;
				}
				if(i==beans.size()-1) {
					//设置同一个用户，最后一次页面的设置
					rv.set(sessionID+
							"\001"+key.toString()+
							"\001"+b1.getUser()+
							"\001"+b1.getTime()+
							"\001"+b1.getRequest()+
							"\001"+step+
							"\001"+(60)+
							"\001"+b1.getHttp_referer()+
							"\001"+b1.getUser_agent()+
							"\001"+b1.getBody_bytes_sent()+
							"\001"+b1.getStatus_code());
					context.write(rk, rv);
				}
			}
		}
		
		//求两个时间的差
		private long timeDiff(Date date, Date date2) {
			return date.getTime()-date2.getTime();
		}
		//将字符串转化成时间
		private Date toDate(String time) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
			try {
				return df.parse(time);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return null;
		}
	}
}
