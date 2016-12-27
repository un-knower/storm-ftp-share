package com.bonc.storm.hdfs2ftp.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.slf4j.LoggerFactory;

/**
 * 加载配置文件
 * */
public class LoadResourceFile {
	
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LoadResourceFile.class);
	// 加载配置文件
	private static CompositeConfiguration config = null;
	// 用来存放配置信息
	private static Map<String, Object> resourceMap = null;
	// 默认配置文件
	private static String STROM_DEFAULT = "config-company.xml";
	// 设置文件策略更新时间
	private static long DEFAULT_RELOAD_TIME = 0L;
	// 
	private static List<String> deprecationList = null;
	static {
		deprecationList = new ArrayList<String>();
		deprecationList.add("property.name");
		deprecationList.add("property.value");
		deprecationList.add("property.description");
		if(config == null) {
			config = new CompositeConfiguration();
		}
		if(resourceMap == null) {
			resourceMap = new HashMap<String, Object>();
		}
	}
	
	public LoadResourceFile() {
		// 初始化
		init();
	}
	
	public LoadResourceFile(String defaultPath) {
		STROM_DEFAULT = defaultPath;
		// 初始化
		init();
	}

	/**
	 * 初始化
	 * */
	public synchronized void init() {
		// 加载默认配置文件
		loadXML(STROM_DEFAULT, false);
		String reloadTime = config.getString("storm.reload.interval");
		if(!"".equals(reloadTime) && reloadTime != null) {
			DEFAULT_RELOAD_TIME = Long.parseLong(reloadTime);
		}
		// 加载默认配置文件中配置到的其他配置文件
		appendBunch();
		// 从CompositeConfiguration中读取出配置文件具体信息并加载到resourceMap中
		extract();
	}
	
	/**
	 * 加载 XML 配置文件
	 * @param fileName
	 * @param isReload 是否设置文件加载策略
	 * */
	public static void loadXML(String fileName, boolean isReload){
		try {
			// 配置文件中值得分隔符
			char ch = '\u0101';
			XMLConfiguration.setDefaultListDelimiter(ch);
			XMLConfiguration xmlConf = new XMLConfiguration(fileName);
			xmlConf.setEncoding("UTF-8");
			if(isReload) {
				if(DEFAULT_RELOAD_TIME != 0L) {
					// 设置文件加载策略
					FileChangedReloadingStrategy fcrs = new FileChangedReloadingStrategy();
					fcrs.setRefreshDelay(DEFAULT_RELOAD_TIME);
					xmlConf.setReloadingStrategy(fcrs);
					xmlConf.reload();
					LOG.debug(fileName+" 设置文件加载策略......");
				}
			}
			config.append(xmlConf);
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 从Properties或XML提取出配置文件内容
	 * */
	@SuppressWarnings("unchecked")
	public static void extract() {
		// XML
		List<String> keyList = config.getList("property.name");
		List<String> valueList = config.getList("property.value");
		if(keyList.size() > 0 && keyList != null) {
			for(int i = 0, j = 0; i < keyList.size() || j < valueList.size(); i++, j++) {
				if(valueList.size() > i) {
					resourceMap.put(keyList.get(i), valueList.get(i));
				}
			}
		}
		// properties及其他
		Iterator<String> list = config.getKeys();
		while(list.hasNext()) {
			String key = list.next();
			if(!deprecationList.contains(key)) {
				String value = config.getString(key);
				resourceMap.put(key, value);
			}
		}
	}
	
	/**
	 * 从XMLConfiguration提取出配置文件内容
	 * @param xmlConf
	 * */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> extractXml(XMLConfiguration xmlConf) {
		Map<String, Object> map = new HashMap<String, Object>();
		List<String> keyList = xmlConf.getList("property.name");
		List<String> valueList = xmlConf.getList("property.value");
		if(keyList.size() > 0 && keyList != null) {
			for(int i = 0, j = 0; i < keyList.size() || j < valueList.size(); i++, j++) {
				if(valueList.size() > i) {
					map.put(keyList.get(i), valueList.get(i));
				}
			}
		}
		return map;
	}
	
	/**
	 * 从PropertiesConfiguration提取出配置文件内容
	 * @param propConf
	 * */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> extractpProp(PropertiesConfiguration propConf) {
		Map<String, Object> map = new HashMap<String, Object>();
		Iterator<String> iterator = propConf.getKeys();
		while(iterator.hasNext()) {
			String key = iterator.next().trim();
			String value = propConf.getString(key);
			map.put(key, value);
		}
		return map;
	}
	
	/** 
	 * 加载 properties 配置文件
	 * @param fileName
	 * @param isReload 是否设置文件在策略
	 * */
	public static void loadProperties(String fileName, boolean isReload){
		try {
			// 配置文件中值得分隔符
			char ch = '\u0101';
			PropertiesConfiguration.setDefaultListDelimiter(ch);
			PropertiesConfiguration propertiesConf = new PropertiesConfiguration(fileName);
			propertiesConf.setEncoding("UTF-8");
			if(isReload) {
				if(DEFAULT_RELOAD_TIME != 0L) {
					// 设置文件加载策略
					FileChangedReloadingStrategy fcrs = new FileChangedReloadingStrategy();
					fcrs.setRefreshDelay(DEFAULT_RELOAD_TIME);
					propertiesConf.setReloadingStrategy(fcrs);
					propertiesConf.reload();
					LOG.debug(fileName+" 设置文件加载策略......");
				}
			}
			config.append(propertiesConf);
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 加载配置在strom-default.xml 中的其他配置文件
	 * */
	@SuppressWarnings("unchecked")
	public static void appendBunch(){
		List<String> propList = config.getList("include-xml");
		// 判断默认配置文件中是否存在xml文件
		if(propList.size() > 0 && propList != null) {
			for(String str : propList){
				loadXML(str, true);
			}
		}
		
		propList = config.getList("include-properties");
		// 判断默认配置文件中是否存在properties文件
		if(propList.size() > 0 && propList != null) {
			for(String str : propList){
				loadProperties(str, true);
			}
		}
	}
	
	/**
	 * 返回配置信息
	 * */
	public Map<String, Object> getResourceMap(){
		return resourceMap;
	}
	
	public static void main(String[] args) {
//		LoadResourceFile load = new LoadResourceFile();
//		Map<String, Object> map = load.getResourceMap();
//		for (Map.Entry<String, Object> entry : map.entrySet()) {
//			System.out.println(entry.getKey() + "   ：   " + (String)entry.getValue());
//		}
		LOG.info("sfasfsd {}"," 设置文件加载策略...... ","dfggj");
		LOG.error("使用正则 {} 匹配字符 {} 异常：","a", "b", new Exception());
	}
}
