package com.bonc.storm.hdfs2ftp.schema;

import java.util.HashMap;

import org.apache.storm.kafka.trident.selector.KafkaTopicSelector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonc.storm.hdfs2ftp.bean.MessageType;



public class MessageTypeTopicSelector implements KafkaTopicSelector {
	
    private static final Logger LOG = LoggerFactory.getLogger(MessageTypeTopicSelector.class);

	private String msgTypeFieldName;
	
	private HashMap<MessageType, String> msgTypeTopicMap;
	
	public MessageTypeTopicSelector(String msgTypeFieldName,
			HashMap<MessageType, String> msgTypeTopicMap) {
		
		this.msgTypeFieldName = msgTypeFieldName;
		
		this.msgTypeTopicMap = msgTypeTopicMap;
		
	}

	public String getTopic(TridentTuple tuple) {
		Object obj = null;
		String topicName = null;
		
		try {
			obj = tuple.getValueByField(msgTypeFieldName);
			if(obj != null && (topicName = this.msgTypeTopicMap.get((MessageType)obj)) != null){
				LOG.info("send message to topic:{}, {}", topicName, tuple.toString());

				return topicName;
			}else{
				LOG.info("未找到 {} 的topic", obj);
			}
		} catch (Exception e) {
			LOG.error("查询 {} 的topic失败：", obj, e);
		}
		
		return null;
	}
	
}
