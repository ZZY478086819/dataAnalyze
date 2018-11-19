package com.zy.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class webLogBean implements Writable{
	/*
	 * 58.248.178.212 
	 * - 
	 * - 
	 * [18/Sep/2013:06:51:37
	 * +0000]
	 * "GET 
	 * /nodejs-grunt-intro/ 
	 * HTTP/1.1" 
	 * 200 
	 * 51770
	 * "http://blog.fens.me/series-nodejs/" 
	 * "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; MDDR; InfoPath.2; .NET4.0C)"
	 */
	
	private boolean vaild =true; //判断数据是否合法
	private String ip_addr;      //记录客户端的ip地址
	private String user ;        //记录客户端用户名 忽略“-”属性
	private String time;         //记录客户端访问时间和时区
	private String request;      //请求的URL和协议
	private String status_code;  //请求的状态码
	private String body_bytes_sent;  //记录发送给客户端文件主体内容的大小（byte）
	private String http_referer;    //记录从哪个页面链接访问过来的
	private String user_agent;    //记录客户端浏览器新的相关信息
	
	public webLogBean() {
	}
	public webLogBean(String ip_addr, String user, String time, String request, String status_code,
			String body_bytes_sent, String http_referer, String user_agent) {
		this.ip_addr = ip_addr;
		this.user = user;
		this.time = time;
		this.request = request;
		this.status_code = status_code;
		this.body_bytes_sent = body_bytes_sent;
		this.http_referer = http_referer;
		this.user_agent = user_agent;
	}
	//重新定义toString方法，将每一个元素按照\001切分
	@Override
	public String toString() {
		StringBuilder  sb=new StringBuilder();
		sb.append(this.vaild);
		sb.append("\001").append(this.ip_addr);
		sb.append("\001").append(this.user);
		sb.append("\001").append(this.time);
		sb.append("\001").append(this.request);
		sb.append("\001").append(this.status_code);
		sb.append("\001").append(this.body_bytes_sent);
		sb.append("\001").append(this.http_referer);
		sb.append("\001").append(this.user_agent);
		return sb.toString();
	}
	public boolean isVaild() {
		return vaild;
	}
	public void setVaild(boolean vaild) {
		this.vaild = vaild;
	}
	public String getIp_addr() {
		return ip_addr;
	}
	public void setIp_addr(String ip_addr) {
		this.ip_addr = ip_addr;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getRequest() {
		return request;
	}
	public void setRequest(String request) {
		this.request = request;
	}
	public String getStatus_code() {
		return status_code;
	}
	public void setStatus_code(String status_code) {
		this.status_code = status_code;
	}
	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}
	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}
	public String getHttp_referer() {
		return http_referer;
	}
	public void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}
	public String getUser_agent() {
		return user_agent;
	}
	public void setUser_agent(String user_agent) {
		this.user_agent = user_agent;
	}
	/**
	 * 序列化方法
	 * @param out
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(this.vaild);
		out.writeUTF(this.ip_addr==null?"":this.ip_addr);
		out.writeUTF(this.user==null?"":this.user);
		out.writeUTF(this.time==null?"":this.time);
		out.writeUTF(this.request==null?"":this.request);
		out.writeUTF(this.status_code==null?"":this.status_code);
		out.writeUTF(this.body_bytes_sent==null?"":this.body_bytes_sent);
		out.writeUTF(this.http_referer==null?"":this.http_referer);
		out.writeUTF(this.user_agent==null?"":this.user_agent);
	}
	/**
	 * 反序列化方法
	 * @param in
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.vaild=in.readBoolean();
		this.ip_addr = in.readUTF();
		this.user = in.readUTF();
		this.time = in.readUTF();
		this.request = in.readUTF();
		this.status_code = in.readUTF();
		this.body_bytes_sent = in.readUTF();
		this.http_referer = in.readUTF();
		this.user_agent = in.readUTF();;
	}
}
