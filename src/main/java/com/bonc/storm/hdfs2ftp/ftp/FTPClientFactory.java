package com.bonc.storm.hdfs2ftp.ftp;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonc.storm.hdfs2ftp.util.FTPClientException;

public class FTPClientFactory implements KeyedPoolableObjectFactory<String, FTPClient>{
	
	private static final Logger LOG = LoggerFactory.getLogger(FTPClientFactory.class);
	// FTP主机信息
	private Map<String, Map<String, String>> ftpInfoMap = null;
	// 配置文件信息
	private Map<String, String> configMap = null;
	int controlKeepAliveReplyTimeout = -1;
	int controlKeepAliveTimeout = -1;
	
	public FTPClientFactory(Map<String, Map<String, String>> ftpInfoMap, Map<String, String> configMap) {
		this.ftpInfoMap = ftpInfoMap;
		this.configMap = configMap;
		controlKeepAliveReplyTimeout = Integer.parseInt(configMap.get("controlKeepAliveReplyTimeout"));
		controlKeepAliveTimeout = Integer.parseInt(configMap.get("controlKeepAliveTimeout"));
	}
	
	/**
	 * 创建一个 FTPClient 
	 * @see org.apache.commons.pool.KeyedPoolableObjectFactory#makeObject(java.lang.Object)
	 */
	public FTPClient makeObject(String key) throws Exception {
		LOG.info("Login FTP：{} ......", key);
		Map<String, String> ftpInfo = ftpInfoMap.get(key);
		FTPClient ftpClient = new FTPClient();
		ftpClient.setConnectTimeout(Integer.parseInt(configMap.get("ftp.connect.timeout")));
		try {
			ftpClient.connect(ftpInfo.get("ip"), Integer.parseInt(ftpInfo.get("ftp_port")));
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
				LOG.error("FTPServer refused connection");
				return null;
			}
			boolean result = ftpClient.login(ftpInfo.get("ftp_name"), ftpInfo.get("ftp_pwd"));
			if (!result) {
				LOG.error("FtpClient login failure！ftp:{}, userName: {} , password: {}", ftpInfo.get("ip"), ftpInfo.get("ftp_name"), ftpInfo.get("ftp_pwd"), new FTPClientException("ftpClient 登陆失败！"));
//				throw new FTPClientException("ftpClient登陆失败! userName:" + ftpInfo.getFtpName() + " ; password:" + ftpInfo.getFtpPwd());
			}
			
			// 等待应答时间 
			ftpClient.setControlKeepAliveReplyTimeout(controlKeepAliveReplyTimeout);
			// 等待发送，超时时间
			ftpClient.setControlKeepAliveTimeout(controlKeepAliveTimeout);
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			ftpClient.setFileTransferMode(FTP.COMPRESSED_TRANSFER_MODE);
			// 设置默认缓冲区大小
			ftpClient.setBufferSize(Integer.parseInt(configMap.get("ftp.buffer.size")));
			ftpClient.setControlEncoding(configMap.get("ftp.encoding"));
			ftpClient.setDataTimeout(Integer.parseInt(configMap.get("ftp.data.timeout")));
			ftpClient.setSoTimeout(Integer.parseInt(configMap.get("ftp.so.timeout")));
			ftpClient.setKeepAlive(true);
			if ("true".equals(configMap.get("ftp.passivemode"))) {
				ftpClient.enterLocalPassiveMode();
			}else{
				ftpClient.enterLocalActiveMode();
			}
			LOG.info("Login FTP {} success......", key);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ftpClient;
	}

	/**
	 * 销毁FTP客户端连接
	 * @see org.apache.commons.pool.KeyedPoolableObjectFactory#destroyObject(java.lang.Object, java.lang.Object)
	 */
	public void destroyObject(String key, FTPClient ftpClient) throws Exception {
		try {
			if (ftpClient != null && ftpClient.isConnected()) {
				ftpClient.logout();
			}
		} catch (IOException io) {
			io.printStackTrace();
		} finally {
			// 注意,一定要在finally代码中断开连接，否则会导致占用ftp连接情况
			if(ftpClient != null && ftpClient.isConnected()) {
				try {
					ftpClient.disconnect();
				} catch (IOException io) {
					io.printStackTrace();
				}
			}
		}
	}

	/**
	 * 验证连接
	 * @see org.apache.commons.pool.KeyedPoolableObjectFactory#validateObject(java.lang.Object, java.lang.Object)
	 */
	public boolean validateObject(String key, FTPClient ftpClient) {
		try {
			return ftpClient.sendNoOp();
		} catch (IOException e) {
			throw new RuntimeException("Failed to validate client: " + e, e);
		}
	}

	public void activateObject(String key, FTPClient client) throws Exception {
		
	}

	public void passivateObject(String key, FTPClient client) throws Exception {
		
	}


}
