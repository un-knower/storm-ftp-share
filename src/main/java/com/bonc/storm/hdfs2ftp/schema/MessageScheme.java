package com.bonc.storm.hdfs2ftp.schema;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonc.storm.hdfs2ftp.bean.JSONBean;
import com.bonc.storm.hdfs2ftp.bean.Message;

import net.sf.json.JSONObject;

public class MessageScheme implements Scheme {
	
	private static final long serialVersionUID = 7791196219148716721L;
	private static final Logger LOG = LoggerFactory.getLogger(MessageScheme.class);
	public static final String MESSAGE_SCHEME_KEY = "hdfs2ftp_msg";
	private String messageDelimiter = "|";
	
	public MessageScheme(String messageDelimiter) {
		this.messageDelimiter = messageDelimiter;
	}
	
	public List<Object> deserialize(byte[] ser) {
		String message = null;
		try {
			message = new String(ser, "UTF-8");
			return new Values(message);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 把topic消息反序列化为Message对象
	 * @param ser
	 * @return
	 */
	private Message toMessage(byte[] ser) {
		Message message = new Message();
		try {
			String serStr = new String(ser, "UTF-8");
			//LOG.info("Received topic's message : {}", serStr);
			if(serStr.startsWith("{") && serStr.endsWith("}")) {
				JSONObject jsonObj = JSONObject.fromObject(serStr);
				JSONBean jsonBean = new JSONBean(jsonObj);

				String fileName = jsonBean.getString("file_name");
				String filePath = jsonBean.getString("file_path");
				String provId = jsonBean.getString("prov_id");
				String opTime = jsonBean.getString("op_time");

				if(fileName == null || filePath == null || provId == null || opTime == null){
					LOG.info("接收到的消息没有包含所有必须字段 file_name、file_path、begin_time、end_time，抛弃！");
					return null;
				}else{
					Message msg = new Message();
					msg.setFileName(fileName);
					msg.setFileOptime(jsonBean.getString("file_optime"));
					msg.setTid(jsonBean.getString("tid"));
					msg.setOperTime(jsonBean.getString("oper_time"));
					msg.setOpTime(opTime);
					msg.setProvId(provId);
					msg.setFilePath(filePath);
					msg.setBeginTime(jsonBean.getString("begin_time"));
					msg.setEndTime(jsonBean.getString("end_time"));
					return msg;
				}
			}else {
				String[] serArr = serStr.split(messageDelimiter, -1);
				if(serArr.length > 5) {
					Message msg = new Message();
					msg.setFileName(serArr[3]);
					msg.setFileOptime(serArr[4]);
					msg.setTid(serArr[1]);
					msg.setOperTime(serArr[10]);
					msg.setOpTime(serArr[0]);
					msg.setProvId(serArr[2]);
					msg.setFilePath(serArr[11]);
					msg.setBeginTime(serArr[7]);
					msg.setEndTime(serArr[8]);
					return msg;
				}else{
					LOG.info("数据异常：{} ", serStr);
					return null;
				}

			}
		} catch (Exception e) {
			LOG.error("转换 JSON 消息异常：", e);
		}
		return message;
	}

	public List<Object> deserialize(ByteBuffer ser) {
		return deserialize(ser.array());
	}

	public Fields getOutputFields() {
        return new Fields(MESSAGE_SCHEME_KEY);
	}

}
