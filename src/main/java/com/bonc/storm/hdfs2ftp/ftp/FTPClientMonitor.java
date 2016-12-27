package com.bonc.storm.hdfs2ftp.ftp;

import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPClientMonitor implements Runnable{
	
	private static final Logger LOG = LoggerFactory.getLogger(FTPClientMonitor.class);
	
	private KeyedObjectPool pool;
	private KeyedPoolableObjectFactory factory;
	private Map<String, Map<String, String>> ftpInfoMap;
	// 每次监视验证的时间间隔
	private long sleepTimeInterval = 10000;
	
	public FTPClientMonitor(KeyedObjectPool pool, 
			KeyedPoolableObjectFactory factory, 
			Map<String, Map<String, String>> ftpInfoMap, long sleepTimeInterval){
		this.pool = pool;
		this.factory = factory;
		this.ftpInfoMap = ftpInfoMap;
		this.sleepTimeInterval = sleepTimeInterval;
	}
	
	public void run() {
		while(true) {
			try {
				String hostKey = null;
				FTPClient ftpClient = null;
				for(Map.Entry<String, Map<String, String>> entry : ftpInfoMap.entrySet()) {
					hostKey = entry.getKey();
					ftpClient = (FTPClient) pool.borrowObject(hostKey);
					boolean noop = ftpClient.sendNoOp();
					LOG.info("hostKey : {}, noop : {} ......", hostKey, noop);
				}
				Thread.sleep(sleepTimeInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (NoSuchElementException e) {
				e.printStackTrace();
			} catch (IllegalStateException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
