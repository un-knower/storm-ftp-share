package com.bonc.storm.hdfs2ftp.ftp;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPClientPool2 implements KeyedObjectPool<String, FTPClient>{

	private static final Logger LOG = LoggerFactory.getLogger(FTPClientPool2.class);
	// 连接池
	private final Map<String, LinkedList<FTPClient>> pool;
	// 连接工厂
	private KeyedPoolableObjectFactory<String, FTPClient> factory;
	private Map<String, Map<String, String>> ftpInfoMap = null;
	// 初始化大小
	private int initSize = 1;
	// 最大连接数
	private int maxSize = 3;
	// ftp连接数
	private int activeSize = 10;
	public FTPClientPool2(Map<String, Map<String, String>> ftpInfoMap, KeyedPoolableObjectFactory<String, FTPClient> factory, int maxSize){
		this.ftpInfoMap = ftpInfoMap;
		this.pool = new HashMap<String, LinkedList<FTPClient>>();
		this.factory = factory;
		this.maxSize = maxSize;
		init();
	}

	/**
	 * 初始化连接池，需要注入一个工厂来提供 FTPClient 实例
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
		LinkedList<FTPClient> ftpClientList = pool.get(key);
		FTPClient ftpClient = null;
		try{
			if(ftpClientList == null || ftpClientList.size() == 0) {
				// 移除该key对应的连接列表
				pool.remove(key);
				// 重新建立该key所有连接
				addObject(key);
				// 获取相应连接列表
				ftpClientList = pool.get(key); 
			}
			
			// 是否继续获取连接
			boolean isGoOn = true;
			int goOn = 0;
			while(goOn > 3 && isGoOn) {
				if(ftpClientList == null || ftpClientList.size() == 0) {
					// 移除该key对应的连接列表
					pool.remove(key);
					// 重新建立该key所有连接
					addObject(key);
					// 获取相应连接列表
					ftpClientList = pool.get(key); 
				}
				synchronized (ftpClientList) {
					// 从连接列表中获取一个连接
					ftpClient = ftpClientList.poll();
				}
				
				// 验证连接是否有效
				int returnCode = ftpClient.pwd();
				LOG.info("returnCode: {}", returnCode);
				// 验证连接是否有效
				if(returnCode == 503) {
					// 无效
					synchronized (ftpClientList) {
						// 销毁连接
						factory.destroyObject(key, ftpClient);
						// 使对象失效，不再受池管辖
//						invalidateObject(key, ftpClient);
						isGoOn = true;
					}
				}else{
					// 有效
					isGoOn = false;
				}
				goOn++;
			}
		}catch(Exception e){
			LOG.info("FTP connection failure......");
			borrowObject(key);
		}
		return ftpClient;
	}

	/**
	 * 返回一个对象给池 
	 * @see org.apache.commons.pool.KeyedObjectPool#returnObject(java.lang.Object, java.lang.Object)
	 */
	public void returnObject(String key, FTPClient ftpClient) throws Exception {
		if (ftpClient != null) {
			try {
				pool.get(key).offer(ftpClient);
//				factory.destroyObject(key, ftpClient);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 使对象失效，不再受池管辖（必须是已经从池中获得的对象）
	 * @see org.apache.commons.pool.KeyedObjectPool#invalidateObject(java.lang.Object, java.lang.Object)
	 */
	public void invalidateObject(String key, FTPClient ftpClient) throws Exception {
		synchronized (pool) {
//			pool.remove(key);
//			factory.destroyObject(key, ftpClient);
			pool.get(key).remove(ftpClient);
		}
	}

	/**
	 * 生成一个对象（通过工程或其他实现方式），并将其放入连接池中
	 * @see org.apache.commons.pool.KeyedObjectPool#addObject(java.lang.Object)
	 */
	public void addObject(String key) throws Exception, IllegalStateException, UnsupportedOperationException {
		LinkedList<FTPClient> ftpClientList = new LinkedList<FTPClient>();
//		for(int i = 0; i < initSize; i++){
			ftpClientList.add(factory.makeObject(key));
//		}
		pool.put(key, ftpClientList);
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
		LinkedList<FTPClient> ftpClientList = null;
		for (Entry<String, LinkedList<FTPClient>> entry : pool.entrySet()) {
			ftpClientList = entry.getValue();
			for(int i = 0; i < ftpClientList.size(); i++) {
				client = ftpClientList.get(i);
				factory.destroyObject(entry.getKey(), client);
			}
			pool.remove(ftpClientList);
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
