package com.zy.mr;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;

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

import com.zy.bean.PageViewsBean;
import com.zy.bean.VisitBean;

public class ClickStreamVisit {
	public static void main(String[] args) {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration conf = new Configuration();
		try {
			Job job = Job.getInstance(conf);
			job.setJarByClass(ClickStreamVisit.class);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(PageViewsBean.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(VisitBean.class);
			Path inpath = new Path("/user/hadoop/output/setup_2");
			FileInputFormat.addInputPath(job, inpath);
			Path output = new Path("/user/hadoop/output/setup_3");
			if (output.getFileSystem(conf).exists(output)) {
				output.getFileSystem(conf).delete(output, true);
			}
			FileOutputFormat.setOutputPath(job, output);
			boolean isSuccess = job.waitForCompletion(true);
			System.exit(isSuccess ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class MyMapper extends Mapper<LongWritable, Text, Text, PageViewsBean> {
		Text mk = new Text();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, PageViewsBean>.Context context)
				throws IOException, InterruptedException {
			/**
			 * c69e145d-67b5-4f46-a642-3df9839a9e9d 0 1.162.203.134 1 - 2 2013-09-18
			 * 13:47:35 3 /images/my.jpg 4 1 5 60 6 "http://www.angularjs.cn/A0d9" 7
			 * "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML,likeGecko)Chrome/29.0.1547.66Safari/537.36"
			 * 19939 9 200 10
			 */
			String line = value.toString();
			String[] fields = line.split("\001");
			int step = Integer.parseInt(fields[5]);
			PageViewsBean pvBean = new PageViewsBean(fields[0], fields[1], fields[3], fields[4], step, fields[6],
					fields[7], fields[8], fields[9], fields[10]);
			mk.set(pvBean.getSessionID());
			context.write(mk, pvBean);
		}
	}

	private static class MyReducer extends Reducer<Text, PageViewsBean, NullWritable, VisitBean> {
		NullWritable rk = NullWritable.get();

		@Override
		protected void reduce(Text key, Iterable<PageViewsBean> values,
				Reducer<Text, PageViewsBean, NullWritable, VisitBean>.Context context)
				throws IOException, InterruptedException {
			List<PageViewsBean> beans = new ArrayList<>();
			for (PageViewsBean v : values) {
				PageViewsBean bean = new PageViewsBean();
				try {
					BeanUtils.copyProperties(bean, v);
					beans.add(bean);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
			Collections.sort(beans, new Comparator<PageViewsBean>() {
				@Override
				public int compare(PageViewsBean o1, PageViewsBean o2) {
					return o1.getStep() - o2.getStep();
				}
			});
			/**
			 * private String sessionID; private String ip_addr; private String time;
			 * private String request; private int step; private String staylong; private
			 * String referal; private String useragent; private String bytes_send; private
			 * String status;
			 */
			VisitBean visitBean = new VisitBean();
			// 取visit的首记录
			visitBean.setInPage(beans.get(0).getRequest());
			visitBean.setInTime(beans.get(0).getTime());
			// 取visit的尾记录
			visitBean.setOutPage(beans.get(beans.size() - 1).getRequest());
			String OutTime=beans.get(beans.size() - 1).getTime();
			System.out.println(beans.size());
			if(beans.size()==1) {
				OutTime=addDate(OutTime,60*1000);
			}
			visitBean.setOutTime(OutTime);
			// visit访问的页面数
			visitBean.setPageVisits(beans.size());
			// 来访者的ip
			visitBean.setRemote_addr(beans.get(0).getIp_addr());
			// 本次visit的referal
			visitBean.setReferal(beans.get(0).getReferal());
			// 本次visit的sessionID
			visitBean.setSession(key.toString());
			context.write(rk, visitBean);
		}

		private String addDate(String outTime, int minute) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
			Date parse=null;
			try {
				parse = new Date(df.parse(outTime).getTime()+minute);
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return df.format(parse);
		}
	}
}