package com.zy.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import com.zy.bean.webLogBean;

/**
 * 日志解析器：目的是把access_log文件的一行日志，解析成一个webLogBean对象
 * @author zy
 *
 */
public class WebLogParser {
	public static SimpleDateFormat sdf1=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
	public static SimpleDateFormat sdf2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.US);
	public static webLogBean parser(String line) {
		String[] split = line.split(" ");
		webLogBean bean =new webLogBean();
		//判断access.log中这一行中是否有字段缺少 ，如果缺少则省略
		if(split.length>11) {
			//设置IP
			bean.setIp_addr(split[0]);
			//设置User
			bean.setUser(split[2]);
			//设置时间
			String time_local=formatDate(split[3].substring(1));
			if("".equals(time_local)||null==time_local) {
				time_local="-invalid_time-";
			}
			bean.setTime(time_local);
			//设置url
			bean.setRequest(split[6]);
			//设置相应码
			bean.setStatus_code(split[8]);
			//设置用户请求主体内容大小
			bean.setBody_bytes_sent(split[9]);
			//设置客户端从哪个连接跳转
			bean.setHttp_referer(split[10]);
			//如果useragent元素较多，拼接useragent
			if(split.length>12) {
				StringBuilder sb =new StringBuilder();
				for(int i=11;i<split.length;i++){
					sb.append(split[i]);
				}
				bean.setUser_agent(sb.toString());
			}else {
				bean.setUser_agent(split[11]);
			}
			if(Integer.parseInt(bean.getStatus_code())>=400) {
				bean.setVaild(false);
			}
			if("-invalid_time-".equals(bean.getTime())) {
				bean.setVaild(false);
			}
		}else {
			return null;
		}
		/*
		 * 58.248.178.212 0
		 * -  1
		 * -  2
		 * [18/Sep/2013:06:51:37 3
		 * +0000] 4 
		 * "GET  5 
		 * /nodejs-grunt-intro/  6
		 * HTTP/1.1"  7
		 * 200  8
		 * 51770 9
		 * "http://blog.fens.me/series-nodejs/"  10
		 * "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; MDDR; InfoPath.2; .NET4.0C)"
		 */
		
		return bean;
	}
	//用于时间转化  dd/MM/yyyy:HH:mm:ss-----> yyyy-MM-dd HH:mm:ss
	private static String formatDate(String substring) {
		try {
			return sdf2.format(sdf1.parse(substring));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
}
