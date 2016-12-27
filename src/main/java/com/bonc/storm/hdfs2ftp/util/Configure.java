package com.bonc.storm.hdfs2ftp.util;

import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Configure implements Serializable{

    private static final long serialVersionUID = 7254386581543229433L;

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Configure.class);
    // 单例
    private static Configure instance = null;
    // 用来保存配置信息及数据库规则信息
    private static Map<String, Object> resourceMap = null;
    private static String DEFAULT_CONFIG = "config-company.xml";
    
    // 私有默认构造方法
    private Configure() {
        if (resourceMap == null) {
            resourceMap = new HashMap<String, Object>();
        }
    }

    /**
     * 静态工厂方法
     */
    public static synchronized Configure getInstance(String sitePath) {
    	DEFAULT_CONFIG = sitePath;
        if (instance == null) {
            instance = new Configure();
            // 初始化
            init();
        }
        return instance;
    }

    /**
     * 静态工厂方法
     */
    public static synchronized Configure getInstance() {
        if (instance == null) {
            instance = new Configure();
            init();
        }
        return instance;
    }

    /**
     * 初始化
     */
    public static void init() {
        LOG.debug("开始加载配置文件......");
        // 加载配置文件
        LoadResourceFile loadResourceFile = new LoadResourceFile(DEFAULT_CONFIG);
        resourceMap = loadResourceFile.getResourceMap();
        LOG.debug("配置文件加载完成......");
    }

    /**
     * 获取resourceMap
     */
    public Map<String, Object> getResourceMap() {
        return resourceMap;
    }

    /**
     * 根据key获取相应配置信息或者数据库规则信息
     */
    public Object get(String key) {
        return resourceMap.get(key);
    }
    
    /**
     * 可以添加自己的临时配置
     */
    public void put(String key, Object object) {
        resourceMap.put(key, object);
    }

    /**
     * 判断某个键是否已存在
     */
    public boolean containsKey(String key) {
        return resourceMap.containsKey(key);
    }

    /**
     * 移除某key，但不会在配置文件或数据库中删除 <br/>
     * 请谨慎使用！！！！！！
     * */
//    public void remove(String key) {
//    	resourceMap.remove(key);
//    }

    /**
     * 判断某值是否已存在
     */
    public boolean containsValue(Object value) {
        return resourceMap.containsValue(value);
    }

    /**
     * 判断配置信息是否为空
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * 获取大小
     */
    public int size() {
        return resourceMap.size();
    }

    /**
     * 把某map放入resourceMap中
     */
    public void putAll(Map<? extends String, ? extends Object> map) {
        resourceMap.putAll(map);
    }

    /**
     * 根据key获取字符串类型的值，如果为空，则返回默认值
     */
    public String getString(String key, String defaultValue) {
        String value = (String) resourceMap.get(key);
        if (!"".equals(value) && value != null) {
            return value;
        }
        return defaultValue;
    }

    /**
     * 根据key获取list类型的返回值
     */
    public List<?> getList(String key) {
        List<?> list = (List<?>) resourceMap.get(key);
        return list;
    }

    /**
     * 根据key获取字符串类型的值
     */
    public String getString(String key) {
        return (String) resourceMap.get(key);
    }
    
    /**
     * 根据key获取整数类型的值
     */
    public int getInt(String key) {
        return  Integer.parseInt((String) resourceMap.get(key));
    }

    public static void main(String[] args) {
        Configure config = Configure.getInstance();
        String interval = config.getString("zkHost");
        System.out.println(interval);
        // List<String> list = (List<String>) config.get("storm.bolt.protocal");
//    	List<String> list = (List<String>) config.getList("storm.bolt.protocal");
//    	for(String str : list) {
//    		System.out.println(str);
//    	}
    }
}
