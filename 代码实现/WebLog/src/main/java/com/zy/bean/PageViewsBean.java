package com.zy.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PageViewsBean implements Writable{
	/**
	 * c69e145d-67b5-4f46-a642-3df9839a9e9d
	 * 1.162.203.134
	 * -
	 * 2013-09-18 13:47:35
	 * /images/my.jpg
	 * 1
	 * 60
	 * "http://www.angularjs.cn/A0d9"
	 * "Mozilla/5.0(WindowsNT6.1;WOW64)AppleWebKit/537.36(KHTML,likeGecko)Chrome/29.0.1547.66Safari/537.36"
	 * 19939
	 * 200
	 */
	private String sessionID;
	private String ip_addr;
	private String time;
	private String request;
	private int step;
	private String staylong;
	private String referal;
	private String useragent;
	private String bytes_send;
	private String status;
	public PageViewsBean(String sessionID, String ip_addr, String time, String request, int step, String staylong,
			String referal, String useragent, String bytes_send, String status) {
		this.sessionID = sessionID;
		this.ip_addr = ip_addr;
		this.time = time;
		this.request = request;
		this.step = step;
		this.staylong = staylong;
		this.referal = referal;
		this.useragent = useragent;
		this.bytes_send = bytes_send;
		this.status = status;
	}
	
	public PageViewsBean() {
	}
	public String getSessionID() {
		return sessionID;
	}
	public void setSessionID(String sessionID) {
		this.sessionID = sessionID;
	}
	public String getIp_addr() {
		return ip_addr;
	}
	public void setIp_addr(String ip_addr) {
		this.ip_addr = ip_addr;
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
	public int getStep() {
		return step;
	}
	public void setStep(int step) {
		this.step = step;
	}
	public String getStaylong() {
		return staylong;
	}
	public void setStaylong(String staylong) {
		this.staylong = staylong;
	}
	public String getReferal() {
		return referal;
	}
	public void setReferal(String referal) {
		this.referal = referal;
	}
	public String getUseragent() {
		return useragent;
	}
	public void setUseragent(String useragent) {
		this.useragent = useragent;
	}
	public String getBytes_send() {
		return bytes_send;
	}
	public void setBytes_send(String bytes_send) {
		this.bytes_send = bytes_send;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(sessionID);
		out.writeUTF(ip_addr);
		out.writeUTF(time);
		out.writeUTF(request);
		out.writeInt(step);
		out.writeUTF(staylong);
		out.writeUTF(referal);
		out.writeUTF(useragent);
		out.writeUTF(bytes_send);
		out.writeUTF(status);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.sessionID = in.readUTF();
		this.ip_addr = in.readUTF();
		this.time = in.readUTF();
		this.request = in.readUTF();
		this.step = in.readInt();
		this.staylong = in.readUTF();
		this.referal = in.readUTF();
		this.useragent = in.readUTF();
		this.bytes_send = in.readUTF();
		this.status = in.readUTF();
	}
}
