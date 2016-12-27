package com.bonc.storm.hdfs2ftp.ftp;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;

public class SFTPClientPool implements KeyedObjectPool<String, ChannelSftp>{
	
	private static final Logger LOG = LoggerFactory.getLogger(SFTPClientPool.class);
	// 连接池
	private final Map<String, ChannelSftp> pool;
	// 连接工厂
	private KeyedPoolableObjectFactory<String, ChannelSftp> factory;
	private Map<String, Map<String, String>> ftpInfoMap = null;
	
	public SFTPClientPool(Map<String, Map<String, String>> ftpInfoMap, KeyedPoolableObjectFactory<String, ChannelSftp> factory){
		this.ftpInfoMap = ftpInfoMap;
		this.pool = new HashMap<String, ChannelSftp>();
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
	public ChannelSftp borrowObject(String key) throws Exception, NoSuchElementException, IllegalStateException {
		ChannelSftp sftp = pool.get(key);
		try{
			if(sftp.isClosed()) {
				synchronized(pool) {
					// 销毁连接
					factory.destroyObject(key, sftp);
					// 使对象失效，不再受池管辖
					invalidateObject(key, sftp);
					// 重新建立连接
					addObject(key);
				}
			}
		}catch(Exception e) {
			LOG.info("SFTP连接失效......", e);
			synchronized(pool) {
				// 销毁连接
				factory.destroyObject(key, sftp);
				// 使对象失效，不再受池管辖
				invalidateObject(key, sftp);
				// 重新建立连接
				addObject(key);
			}
		}
		return sftp;
	}

	/**
	 * 返回一个对象给池 
	 * @see org.apache.commons.pool.KeyedObjectPool#returnObject(java.lang.Object, java.lang.Object)
	 */
	public void returnObject(String key, ChannelSftp obj) throws Exception {
		
	}

	/**
	 * 使对象失效，不再受池管辖（必须是已经从池中获得的对象）
	 * @see org.apache.commons.pool.KeyedObjectPool#invalidateObject(java.lang.Object, java.lang.Object)
	 */
	public void invalidateObject(String key, ChannelSftp obj) throws Exception {
		synchronized (pool) {
			pool.remove(key);
		}
	}

	/**
	 * 生成一个对象（通过工程或其他实现方式），并将其放入连接池中
	 * @see org.apache.commons.pool.KeyedObjectPool#addObject(java.lang.Object)
	 */
	public void addObject(String key) throws Exception, IllegalStateException, UnsupportedOperationException {
		pool.put(key, factory.makeObject(key));
	}

	public int getNumIdle(String key) throws UnsupportedOperationException {
		return 0;
	}

	public int getNumActive(String key) throws UnsupportedOperationException {
		return 0;
	}

	public int getNumIdle() throws UnsupportedOperationException {
		return 0;
	}

	public int getNumActive() throws UnsupportedOperationException {
		return 0;
	}

	/**
	 * 清空池中空闲对象，释放相关资源
	 * @see org.apache.commons.pool.KeyedObjectPool#clear(java.lang.Object)
	 */
	public void clear() throws Exception, UnsupportedOperationException {
		pool.clear();
	}

	public void clear(String key) throws Exception, UnsupportedOperationException {
		
	}

	/**
	 * 关闭池，释放所有与它相关资源 
	 * @see org.apache.commons.pool.KeyedObjectPool#close()
	 */
	public void close() throws Exception {
		ChannelSftp sftp = null;
		for (Entry<String, ChannelSftp> entry : pool.entrySet()) {
			sftp = entry.getValue();
			factory.destroyObject(entry.getKey(), sftp);
		}
	}

	public void setFactory(KeyedPoolableObjectFactory<String, ChannelSftp> factory)
			throws IllegalStateException, UnsupportedOperationException {
		
	}

}
