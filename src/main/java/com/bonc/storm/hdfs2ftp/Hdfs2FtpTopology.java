package com.bonc.storm.hdfs2ftp;

import com.bonc.storm.hdfs2ftp.bean.MessageType;
import com.bonc.storm.hdfs2ftp.schema.MessageTypeTopicSelector;
import com.bonc.storm.hdfs2ftp.util.Configure;
import com.bonc.storm.hdfs2ftp.util.Hdfs2FtpSQL;
import com.bonc.storm.hdfs2ftp.util.StringUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;



/**
 * 此拓扑是实现从kafka读取topic消息，消息中含有hdfs文件名及路径，根据路径读取文件写入ftp
 * 
 * @author xiabaike
 * @date 2015/12/09 01:53
 */
public class Hdfs2FtpTopology {
	
	private static String topologyName = null;
	private static final Logger LOG = LoggerFactory.getLogger(Hdfs2FtpTopology.class);
	// 加载配置文件
	private static Configure config = null;
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		String usage = "Usage:AuditFileNameTopology [-topologyName <topologyName> -configPath <configPath> -groupID <groupID> -ftpType <ftpType>]";
		
		String configPath = "hdfs2ftp-config.xml";
		String groupId = "";
		String ftpType = "0";
		for(int i = 0 ; i < args.length ; i++){
			if("-topologyName".equals(args[i])){
				topologyName = args[++i];
			}else if("-configPath".equals(args[i])){
				configPath = args[++i];
			}else if("-groupID".equals(args[i])){
				groupId = args[++i];
			}else if("-ftpType".equals(args[i])){
				ftpType = args[++i];
			}
		}
		if( "".equals(groupId) || groupId == null ) {
			System.err.println("参数 groupID 不能为空");
			System.err.println(usage);
			LOG.error("参数 groupID 不能为空");
			System.exit(-1);
		}
		if( !"0".equals(ftpType) && !"1".equals(ftpType) ) {
			ftpType = "0";
		}
		
		// 加载配置文件
		config = Configure.getInstance(configPath);
		
		Hdfs2FtpSQL sql = new Hdfs2FtpSQL(config, groupId);
		if( !sql.isExistGroupId() ) {
			System.err.println("查询不到该 groupID: "+groupId+ " 相关信息， 请确认......");
			System.exit(-1);
		}
		// 查询远程及本地路径
		Map<String, List<Map<String, String>>> shareMap = sql.getSharePath();
		if(shareMap.size() == 0 || shareMap == null) {
			System.err.println("查询不到该 groupID: "+groupId+ " 所对应的远程及本地路径， 请确认......");
			System.exit(-1);
		}
		// 查询FTP主机信息
		Map<String, Map<String, String>> ftpInfoMap = sql.getFTPHostInfoList();
		if(ftpInfoMap.size() == 0 || ftpInfoMap == null) {
			System.err.println("查询不到该 groupID: "+groupId+ " 所对应的FTP主机信息， 请确认......");
			System.exit(-1);
		}
		
		// kafka使用的zookeeper的host
		String zkHost = config.getString("zkHost");
		if(zkHost == null || !StringUtil.checkHost(zkHost)){
			System.err.println("参数配置错误，请检查配置文件中的 zkHost 参数");
			System.exit(-1);
		}
		// spout消费的topic
		String consumerTopic = config.getString("consumerTopic");
		if(consumerTopic == null){
			System.err.println("参数配置错误，请检查配置文件中的 consumerTopic 参数");
			System.exit(-1);
		}
		//正常输出topic normalProducerTopic
		String normalProducerTopic = config.getString("normalProducerTopic");
		if(normalProducerTopic == null){
			System.err.println("参数配置错误，请检查配置文件中的 normalProducerTopic 参数");
			System.exit(-1);
		}
		// 异常输出topic
		String errorProducerTopic = config.getString("errorProducerTopic");
		if(errorProducerTopic == null){
			System.err.println("参数配置错误，请检查配置文件中的 errorProducerTopic 参数");
			System.exit(-1);
		}
		// spout信息保存在zookeeper的路径
		String spoutClientId = config.getString("spoutClientId");
		if(spoutClientId == null || spoutClientId.startsWith("/")){
			System.err.println("参数配置错误，请检查配置文件中的 spoutClientId 参数，spoutClientId不能为空且不能以 / 开头");
			System.exit(-1);
		}
		// kafka接收消息的 broker 地址列表
		String producerBrokerList = config.getString("producerBrokerList");
		if(producerBrokerList == null || !StringUtil.checkHost(producerBrokerList)){
			System.err.println("参数配置错误，请检查配置文件中的 producerBrokerList 参数");
			System.exit(-1);
		}
		// topic消息分隔符
		String messageDelimiter = config.getString("messageDelimiter");
		if(messageDelimiter == null){
			System.err.println("参数配置错误，请检查配置文件中的 messageDelimiter 参数");
			System.exit(-1);
		}
		// MessageTimeoutSecs
		String messageTimeoutSecs = config.getString("messageTimeoutSecs");
		if(messageTimeoutSecs == null){
			messageTimeoutSecs = "300";
		}
		
		// 配置 kafka broker host 与  partition 的  mapping 信息
		BrokerHosts zk = new ZkHosts(zkHost);
		TridentKafkaConfig  spoutConf = new TridentKafkaConfig(zk, consumerTopic, spoutClientId);
		String startOffsetTime = config.getString("startOffsetTime");
		if ("smallest".equals(startOffsetTime)) {
			spoutConf.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		} else if ("largest".equals(startOffsetTime)) {
			spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		}
		// 使用MessageScheme 解析消息并定义输出字段

//		spoutConf.scheme = new SchemeAsMultiScheme(new MessageScheme(messageDelimiter));
		spoutConf.scheme = new SchemeAsMultiScheme(new KafkaScheme(messageDelimiter));
		// kafka是否重头开始消费
		String forceFromStart = config.getString("forceFromStart");
		if(forceFromStart != null){
			try {
				spoutConf.ignoreZkOffsets = Boolean.parseBoolean(forceFromStart);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// SimpleConsumer所使用的SocketChannel的读缓冲区大小
		String bufferSizeBytes = config.getString("bufferSizeBytes");
		if( !"".equals(bufferSizeBytes) && bufferSizeBytes != null ){
			try {
				spoutConf.bufferSizeBytes = Integer.parseInt(bufferSizeBytes);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 发给Kafka的每个FetchRequest中，用此指定想要的response中总的消息的大小
		String fetchSizeBytes = config.getString("fetchSizeBytes");
		if( !"".equals(fetchSizeBytes) && fetchSizeBytes != null ){
			try {
				spoutConf.fetchSizeBytes = Integer.parseInt(fetchSizeBytes);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// 确保每个tuple只在一个batch中被成功处理
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

		TridentTopology topology = new TridentTopology();
		// 设置 bolt 的输出字段
		Fields outputFields = new Fields("msg_type","key","msg");
		// 设置spout和auditBolt
		Stream stream = topology.newStream(spoutClientId, spout).parallelismHint(config.getInt("spoutParallelism")).shuffle()
				.each(spout.getOutputFields(), new Hdfs2FtpFunction(ftpType), outputFields).parallelismHint(config.getInt("auditBoltParallelism")).shuffle();
		
		// 根据消息类型选择topic
		HashMap<MessageType,String> msgTypeTopic = new HashMap<MessageType,String>();

		msgTypeTopic.put(MessageType.ERROR, errorProducerTopic);
		msgTypeTopic.put(MessageType.NORMAL, normalProducerTopic);

		Properties props = new Properties();
		props.put("bootstrap.servers", producerBrokerList);
		props.put("acks", "1");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
				.withProducerProperties(props)
        .withKafkaTopicSelector(new MessageTypeTopicSelector("msg_type", msgTypeTopic))
        .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "msg"));

		// 设置写入kafka
		stream.partitionPersist(stateFactory, outputFields, new TridentKafkaUpdater()).parallelismHint(config.getInt("kafkaUpdateParallelism"));
		
		Config conf = new Config();
		conf.put("hdfs2ftp.config", config.getResourceMap());
		conf.put("share.path", shareMap);
		conf.put("ftp.host.info", ftpInfoMap);
		if(config.containsKey("krb_flag")){
		conf.put("krb_flag",config.getString("krb_flag"));
		conf.put("krb_principal",config.getString("krb_principal"));
		conf.put("krb_keystore",config.getString("krb_keystore"));
        System.out.println("-------------->>>>>>"+config.getString("krb_keystore"));
		}
		// 默认500
		String intervalMillis = config.getString("topology.trident.batch.emit.interval.millis");
		if(intervalMillis != null){
			conf.put("topology.trident.batch.emit.interval.millis", Integer.parseInt(intervalMillis));
		}
		// 默认262144
		String transferBatchSize = config.getString("storm.messaging.netty.transfer.batch.size");
		if(transferBatchSize != null){
			conf.put("storm.messaging.netty.transfer.batch.size", Integer.parseInt(transferBatchSize));
		}
		
		String maxSpoutPending = config.getString("MaxSpoutPending");
		if(maxSpoutPending != null){
			conf.setMaxSpoutPending(Integer.parseInt(maxSpoutPending));
		}
		
		// messageTimeoutSecs
		// spout发送消息的最大超时时间。
		// 如果一条消息在该时间窗口内未被成功ack，storm会告知spout这条消息失败。
		conf.setMessageTimeoutSecs(Integer.parseInt(messageTimeoutSecs));
		
		//set producer properties.
//		Properties props = new Properties();
//		props.put("metadata.broker.list", producerBrokerList);
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
//		String producerType = config.getString("producer.type");
//		if(producerType != null){
//			props.put("producer.type", producerType);
//		}
//
//		String batchSize = config.getString("batch.num.messages");
//		if(batchSize != null){
//			props.put("batch.num.messages", batchSize);
//		}
//
//		String queueTimeout = config.getString("queue.enqueue.timeout.ms");
//		if(queueTimeout != null){
//			props.put("queue.enqueue.timeout.ms", queueTimeout);
//		}
//
//		String acks = config.getString("request.required.acks");
//		if(acks != null){
//			props.put("request.required.acks", acks);
//		}
		
//		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
		conf.setDebug(false);
		Integer numWorkers = config.getInt("numWorkers");
		if(numWorkers != null) {
			conf.setNumWorkers(numWorkers);
		}

		if(topologyName != null){
			StormSubmitter.submitTopology(topologyName, conf, topology.build());
		}else{
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, topology.build());
		}
	}


}
  class KafkaScheme implements Scheme
{
	private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;
	public static final String KAFKA_SCHEME_KEY = "source";
	private String messageDelimiter;

	public KafkaScheme(String messageDelimiter) {
		this.messageDelimiter = messageDelimiter;
	}


	public List<Object> deserialize(ByteBuffer bytes) {
		return new Values(deserializeString(bytes));
	}

	public static String deserializeString(ByteBuffer string) {
		if (string.hasArray()) {
			int base = string.arrayOffset();
			return new String(string.array(), base + string.position(), string.remaining());
		} else {
			return new String(Utils.toByteArray(string), UTF8_CHARSET);
		}
	}

	public Fields getOutputFields() {
		return new Fields(new String[]{KAFKA_SCHEME_KEY});
	}
}