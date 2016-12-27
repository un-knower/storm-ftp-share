package com.bonc.storm.hdfs2ftp.ftp;

import java.util.Map;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class SFTPClientFactory implements KeyedPoolableObjectFactory<String, ChannelSftp>{

	private static final Logger LOG = LoggerFactory.getLogger(SFTPClientFactory.class);
	// FTP主机信息
	private Map<String, Map<String, String>> ftpInfoMap = null;
	// 配置文件信息
	private Map<String, String> configMap = null;
	
	public SFTPClientFactory(Map<String, Map<String, String>> ftpInfoMap, Map<String, String> configMap) {
		this.ftpInfoMap = ftpInfoMap;
		this.configMap = configMap;
	}
	
	/**
	 * 创建一个 FTPClient 
	 * @see org.apache.commons.pool.KeyedPoolableObjectFactory#makeObject(java.lang.Object)
	 */
	public ChannelSftp makeObject(String key) throws Exception {
		LOG.info("尝试连接登录 SFTP ：{} ......", key);
		Session session = null;
		Channel channel = null;
		ChannelSftp sftpChannel = null;
		Map<String, String> ftpInfo = ftpInfoMap.get(key);
		int port = Integer.parseInt(ftpInfo.get("ftp_port"));
		String username = ftpInfo.get("ftp_name");
		String ip = ftpInfo.get("ip");
		String password = ftpInfo.get("ftp_pwd");
		try {
			JSch jsch = new JSch();
			int loginTimeout = Integer.parseInt(configMap.get("sftp.connect.timeout"));
			if(port <= 0){
			    // 连接服务器，采用默认端口
			    session = jsch.getSession(username, ip);
			}else{
			    // 采用指定的端口连接服务器
			    session = jsch.getSession(username, ip ,port);
			}
			// 如果服务器连接不上，则抛出异常
			if (session == null) {
				return null;
			}
			
			// 设置登陆主机的密码
			session.setPassword(password);  
			// 设置第一次登陆的时候提示，可选值：(ask | yes | no)
			session.setConfig("StrictHostKeyChecking", "no");
			// 设置登陆超时时间   
			session.connect(loginTimeout);
			     
			// 创建sftp通信通道
			channel = (Channel) session.openChannel("sftp");
			channel.connect(loginTimeout);
			sftpChannel = (ChannelSftp) channel;
			LOG.info("连接登录 SFTP {} 成功......", key);
		} catch (JSchException e) {
			LOG.info("连接 sftp 服务器 "+ip+" 失败", e);
		}
		return sftpChannel;
	}

	/**
	 * 销毁FTP客户端连接
	 * @see org.apache.commons.pool.KeyedPoolableObjectFactory#destroyObject(java.lang.Object, java.lang.Object)
	 */
	public void destroyObject(String key, ChannelSftp channelSftp) throws Exception {
		Session session = null;
		if (channelSftp != null) {
            if (channelSftp.isConnected()) {  
            	channelSftp.disconnect();
            }
            session = channelSftp.getSession();
        	if (session != null) {
                if (session.isConnected()) {
                    session.disconnect();
                }
            }
        }
	}

	/**
	 * 验证连接
	 * @see org.apache.commons.pool.KeyedPoolableObjectFactory#validateObject(java.lang.Object, java.lang.Object)
	 */
	public boolean validateObject(String key, ChannelSftp obj) {
		return false;
	}

	public void activateObject(String key, ChannelSftp obj) throws Exception {
		
	}

	public void passivateObject(String key, ChannelSftp obj) throws Exception {
		
	}

}
