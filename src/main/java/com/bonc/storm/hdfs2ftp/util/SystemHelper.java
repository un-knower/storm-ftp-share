package com.bonc.storm.hdfs2ftp.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemHelper {

	private static final Logger LOG = LoggerFactory.getLogger(SystemHelper.class);

    public static String getLocalIPForJava(){
        String ip = null;
        try {
        	Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); 
            while (en.hasMoreElements()) {
                NetworkInterface intf = (NetworkInterface) en.nextElement();
                if(intf.getDisplayName().indexOf("VMware Virtual") < 0) {
                	Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses();
                    while (enumIpAddr.hasMoreElements()) {
                         InetAddress inetAddress = (InetAddress) enumIpAddr.nextElement();
                         if (!inetAddress.isLoopbackAddress()  && !inetAddress.isLinkLocalAddress() 
                        		 	&& inetAddress.isSiteLocalAddress()) {
                        	 ip = inetAddress.getHostAddress().toString();
                         }
                     }
                }
              }
        } catch (SocketException e) {
        	LOG.error("获取IP地址失败", e);
        }
        return ip;
    }
    
    public static void main(String[] args) {
//    	try {
			System.out.println(getLocalIPForJava());
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
	}
}
