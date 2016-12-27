package com.bonc.storm.hdfs2ftp.bean;

import java.io.Serializable;

public class Message implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8299457523312128769L;

	private String fileName;
	private String fileOptime;
	private String filePath;
	private String opTime;
	private String operTime;
	private String provId;
	private String tid;
	private String beginTime;
	private String endTime;
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getFileOptime() {
		return fileOptime;
	}
	public void setFileOptime(String fileOptime) {
		this.fileOptime = fileOptime;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	public String getOpTime() {
		return opTime;
	}
	public void setOpTime(String opTime) {
		this.opTime = opTime;
	}
	public String getOperTime() {
		return operTime;
	}
	public void setOperTime(String operTime) {
		this.operTime = operTime;
	}
	public String getProvId() {
		return provId;
	}
	public void setProvId(String provId) {
		this.provId = provId;
	}
	public String getTid() {
		return tid;
	}
	public void setTid(String tid) {
		this.tid = tid;
	}
	public String getBeginTime() {
		return beginTime;
	}
	public void setBeginTime(String beginTime) {
		this.beginTime = beginTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	@Override
	public String toString() {
		return "Message [fileName=" + fileName + ", fileOptime=" + fileOptime + ", filePath=" + filePath + ", opTime="
				+ opTime + ", operTime=" + operTime + ", provId=" + provId + ", tid=" + tid + ", beginTime=" + beginTime
				+ ", endTime=" + endTime + "]";
	}
		
}
