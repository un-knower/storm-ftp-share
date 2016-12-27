package com.bonc.storm.hdfs2ftp.bean;

import java.io.Serializable;

import net.sf.json.JSONObject;

public class FTPMessage implements Serializable{

	private static final long serialVersionUID = 8299457523312128769L;

	/**
	 * 账期
	 */
	private String opTime;
	/**
	 * 文件名称
	 */
	private String 	fileName;
	/**
	 * 省份ID
	 */
	private String provId;
	/**
	 * tid
	 */
	private String tid;
	/**
	 * 上传汇聚平台开始时间
	 */
	private String hjBeginTime;
	/**
	 * 上传汇聚平台结束时间
	 */
	private String hjEndTime;
	/**
	 * 文件大小
	 */
	private String fileSize;
	/**
	 * 漫游下发路径
	 */
	private String remotePath;
	/**
	 * 漫游下发用户
	 */
	private String ftpName;
	/**
	 * 下发主机地址
	 */
	private String remoteIP;
	/**
	 * 下发单位
	 */
	private String unit;
	/**
	 * 本地主机地址
	 */
	private String locIP;
	/**
	 * 本地路径
	 */
	private String locPath;
	/**
	 * 上传开始时间
	 */
	private String beginTime;
	/**
	 * 上传结束时间
	 */
	private String endTime;
	/**
	 * 操作时间
	 */
	private String operTime;
	/**
	 * 异常原因
	 */
	private String remark;
	/**
	 * 是否补传标识
	 */
	private String reloadFlag;
	
	public String getOpTime() {
		return opTime;
	}
	public void setOpTime(String opTime) {
		this.opTime = opTime;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
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
	public String getHjBeginTime() {
		return hjBeginTime;
	}
	public void setHjBeginTime(String hjBeginTime) {
		this.hjBeginTime = hjBeginTime;
	}
	public String getHjEndTime() {
		return hjEndTime;
	}
	public void setHjEndTime(String hjEndTime) {
		this.hjEndTime = hjEndTime;
	}
	public String getFileSize() {
		return fileSize;
	}
	public void setFileSize(String fileSize) {
		this.fileSize = fileSize;
	}
	public String getRemotePath() {
		return remotePath;
	}
	public void setRemotePath(String remotePath) {
		this.remotePath = remotePath;
	}
	public String getFtpName() {
		return ftpName;
	}
	public void setFtpName(String ftpName) {
		this.ftpName = ftpName;
	}
	public String getRemoteIP() {
		return remoteIP;
	}
	public void setRemoteIP(String remoteIP) {
		this.remoteIP = remoteIP;
	}
	public String getUnit() {
		return unit;
	}
	public void setUnit(String unit) {
		this.unit = unit;
	}
	public String getLocIP() {
		return locIP;
	}
	public void setLocIP(String locIP) {
		this.locIP = locIP;
	}
	public String getLocPath() {
		return locPath;
	}
	public void setLocPath(String locPath) {
		this.locPath = locPath;
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
	public String getOperTime() {
		return operTime;
	}
	public void setOperTime(String operTime) {
		this.operTime = operTime;
	}
	public String getRemark() {
		return remark;
	}
	public void setRemark(String remark) {
		this.remark = remark;
	}
	public String getReloadFlag() {
		return reloadFlag;
	}
	public void setReloadFlag(String reloadFlag) {
		this.reloadFlag = reloadFlag;
	}
	
	@Override
	public String toString() {
		return "Message [opTime=" + opTime + ", fileName=" + fileName + ", provId=" + provId + ", remotePath="
				+ remotePath + ", ftpName=" + ftpName + ", remoteIP=" + remoteIP + ", unit=" + unit + ", locIP=" + locIP
				+ ", beginTime=" + beginTime + ", endTime=" + endTime + ", operTime=" + operTime + ", remark=" + remark
				+ ", reload_flag="+ reloadFlag +"]";
	}
	
	public String toJSONComm(){
		StringBuffer sb = new StringBuffer();
		sb.append("{\"op_time\":\"").append(opTime).append("\",").
			append("\"file_name\":\"").append(fileName).append("\",").
			append("\"prov_id\":\"").append(provId).append("\",").
			append("\"tid\":\"").append(tid).append("\",").
			append("\"hj_begin_time\":\"").append(hjBeginTime).append("\",").
			append("\"hj_end_time\":\"").append(hjEndTime).append("\",").
			append("\"file_size\":\"").append(fileSize).append("\",").
			append("\"remote_path\":\"").append(remotePath).append("\",").
			append("\"ftp_name\":\"").append(ftpName).append("\",").
			append("\"remote_ip\":\"").append(remoteIP).append("\",").
			append("\"unit\":\"").append(unit).append("\",").
			append("\"loc_ip\":\"").append(locIP).append("\",").
			append("\"loc_path\":\"").append(locPath).append("\",").
			append("\"begin_time\":\"").append(beginTime).append("\",").
			append("\"end_time\":\"").append(endTime).append("\",").
			append("\"oper_time\":\"").append(operTime).append("\",").
			append("\"reload_flag\":\"").append(reloadFlag).append("\"}");
		return sb.toString();
	}
	
	public String toJSONError(){
		StringBuffer sb = new StringBuffer();
		sb.append("{\"op_time\":\"").append(opTime).append("\",").
			append("\"file_name\":\"").append(fileName).append("\",").
			append("\"prov_id\":\"").append(provId).append("\",").
			append("\"tid\":\"").append(tid).append("\",").
			append("\"hj_begin_time\":\"").append(hjBeginTime).append("\",").
			append("\"hj_end_time\":\"").append(hjEndTime).append("\",").
			append("\"file_size\":\"").append(fileSize).append("\",").
			append("\"remote_path\":\"").append(remotePath).append("\",").
			append("\"ftp_name\":\"").append(ftpName).append("\",").
			append("\"remote_ip\":\"").append(remoteIP).append("\",").
			append("\"unit\":\"").append(unit).append("\",").
			append("\"loc_ip\":\"").append(locIP).append("\",").
			append("\"loc_path\":\"").append(locPath).append("\",").
			append("\"begin_time\":\"").append(beginTime).append("\",").
			append("\"end_time\":\"").append(endTime).append("\",").
			append("\"remark\":\"").append(remark).append("\",").
			append("\"oper_time\":\"").append(operTime).append("\",").
			append("\"reload_flag\":\"").append(reloadFlag).append("\"}");
		return sb.toString();
	}
	
	public static void main(String[] args) {
		FTPMessage ftpMsg = new FTPMessage();
		ftpMsg.setReloadFlag("dd");
		JSONObject jsonObj = JSONObject.fromObject(ftpMsg.toJSONComm());
		System.out.println(jsonObj.toString());
		String str = "|dfsafasd";
		System.out.println(str.indexOf("d"));
	}
}
