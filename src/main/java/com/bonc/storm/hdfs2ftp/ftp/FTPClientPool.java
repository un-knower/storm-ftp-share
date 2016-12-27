package com.bonc.storm.hdfs2ftp.ftp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPClientPool implements KeyedObjectPool<String, FTPClient>{

	private static final Logger LOG = LoggerFactory.getLogger(FTPClientPool.class);
	// 连接池
	private final Map<String, FTPClient> pool;
	// 连接工厂
	private KeyedPoolableObjectFactory<String, FTPClient> factory;
	private Map<String, Map<String, String>> ftpInfoMap = null;
	private boolean flag = false;
	public FTPClientPool(Map<String, Map<String, String>> ftpInfoMap, KeyedPoolableObjectFactory<String, FTPClient> factory){
		this.ftpInfoMap = ftpInfoMap;
		this.pool = new HashMap<String, FTPClient>();
		this.factory = factory;
		init();
	}

	/**
	 * 初始化连接池，需要注入一个工厂来提供FTPClient实例
	 */
	public void init() {
		try {
			String key = null;
			for (Entry<String, Map<String, String>> entry : ftpInfoMap.entrySet()) {
				key = entry.getKey();
				addObject(key);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 从池中获得一个对象
	 * @see org.apache.commons.pool.KeyedObjectPool#borrowObject(java.lang.Object)
	 */
	public FTPClient borrowObject(String key) throws Exception, NoSuchElementException, IllegalStateException {
		FTPClient ftpClient = pool.get(key);
		
//		synchronized(ftpClient) {
//			if(flag) {
//				ftpClient.wait();
//			}else{
//				flag = true;
//			}
			try{
				//LOG.info("ftpClient.isConnected() : {} ", ftpClient.isConnected());
				//LOG.info("ftpClient.noop(): {}", ftpClient.noop());
				int returnCode = ftpClient.pwd();
				LOG.info("returnCode: {}", returnCode);
				
				// 验证连接是否有效
				if(returnCode != 257) {
//					synchronized (pool) {
						// 销毁连接
						factory.destroyObject(key, ftpClient);
						// 使对象失效，不再受池管辖
						invalidateObject(key, ftpClient);
						// 重新建立连接
						addObject(key);
						ftpClient = pool.get(key) ;
//					}
				}
			}catch(Exception e){
				e.printStackTrace();
				LOG.info("FTP connection failure......");
//				synchronized (pool) {
					// 销毁连接
					factory.destroyObject(key, ftpClient);
					// 使对象失效，不再受池管辖
					invalidateObject(key, ftpClient);
					// 重新建立连接
					addObject(key);
					ftpClient = pool.get(key) ;
//				}
			}
//			flag = false;
//			ftpClient.notifyAll();
//		}
		return ftpClient;
	}

	/**
	 * 返回一个对象给池 
	 * @see org.apache.commons.pool.KeyedObjectPool#returnObject(java.lang.Object, java.lang.Object)
	 */
	public void returnObject(String key, FTPClient ftpClient) throws Exception {
		if (ftpClient != null) {
			try {
				factory.destroyObject(key, ftpClient);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 使对象失效，不再受池管辖（必须是已经从池中获得的对象）
	 * @see org.apache.commons.pool.KeyedObjectPool#invalidateObject(java.lang.Object, java.lang.Object)
	 */
	public void invalidateObject(String key, FTPClient ftpClient) throws Exception {
//		synchronized (pool) {
			pool.remove(key);
//		}
	}

	/**
	 * 生成一个对象（通过工程或其他实现方式），并将其放入连接池中
	 * @see org.apache.commons.pool.KeyedObjectPool#addObject(java.lang.Object)
	 */
	public void addObject(String key) throws Exception, IllegalStateException, UnsupportedOperationException {
		pool.put(key, factory.makeObject(key));
	}

	/**
	 * 获得空闲对象的数量
	 * @see org.apache.commons.pool.KeyedObjectPool#getNumIdle(java.lang.Object)
	 */
	public int getNumIdle(String key) throws UnsupportedOperationException {
		return 0;
	}

	/**
	 * 获得活动对象的数量
	 * @see org.apache.commons.pool.KeyedObjectPool#getNumActive(java.lang.Object)
	 */
	public int getNumActive(String key) throws UnsupportedOperationException {
		return 0;
	}

	public int getNumIdle() throws UnsupportedOperationException {
		return 0;
	}

	public int getNumActive() throws UnsupportedOperationException {
		return 0;
	}

	public void clear() throws Exception, UnsupportedOperationException {
		
	}

	/**
	 * 清空池中空闲对象，释放相关资源
	 * @see org.apache.commons.pool.KeyedObjectPool#clear(java.lang.Object)
	 */
	public void clear(String key) throws Exception, UnsupportedOperationException {
		pool.clear();
	}

	/**
	 * 关闭池，释放所有与它相关资源 
	 * @see org.apache.commons.pool.KeyedObjectPool#close()
	 */
	public void close() throws Exception {
		FTPClient client = null;
		for (Entry<String, FTPClient> entry : pool.entrySet()) {
			client = entry.getValue();
			factory.destroyObject(entry.getKey(), client);
		}
	}

	/**
	 * 设置池对象工厂
	 * @see org.apache.commons.pool.ObjectPool#setFactory(org.apache.commons.pool.PoolableObjectFactory)
	 */
	public void setFactory(KeyedPoolableObjectFactory<String, FTPClient> factory)
			throws IllegalStateException, UnsupportedOperationException {
//		this.factory = factory;
	}
	
}
