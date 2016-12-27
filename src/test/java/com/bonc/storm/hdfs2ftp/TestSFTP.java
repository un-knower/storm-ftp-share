package com.bonc.storm.hdfs2ftp;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.KeyedPoolableObjectFactory;

import com.bonc.storm.hdfs2ftp.ftp.SFTPClientFactory;
import com.bonc.storm.hdfs2ftp.ftp.SFTPClientPool;
import com.jcraft.jsch.ChannelSftp;

public class TestSFTP {

	// 连接工厂
	private static KeyedPoolableObjectFactory<String, ChannelSftp> factory;
	private static Map<String, Map<String, String>> ftpInfoMap = new HashMap<String, Map<String, String>>();
	// 配置文件信息
	private static Map<String, String> configMap = null;
	private static String key = "51";
	
	public static void main(String[] args) {
		Map<String,String> map = new HashMap<String, String>();
		map.put("ftp_name", "bonc");
		map.put("ip", "192.168.8.51");
		map.put("ftp_pwd", "bonc");
		map.put("ftp_port", "22");
		ftpInfoMap.put(key, map);
		configMap = new HashMap<String, String>();
		configMap.put("ftp.connect.timeout", "10000");
		factory = new SFTPClientFactory(ftpInfoMap, configMap);
		KeyedObjectPool<String, ChannelSftp> pool = new SFTPClientPool(ftpInfoMap, factory);
		try {
			Thread.sleep(100000);
			ChannelSftp sftp = pool.borrowObject(key);
			
			System.out.println(sftp.getHome());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
