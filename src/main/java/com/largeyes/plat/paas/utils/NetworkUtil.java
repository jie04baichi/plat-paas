package com.largeyes.plat.paas.utils;

import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;


public class NetworkUtil {
    public static final Logger log = Logger.getLogger(NetworkUtil.class);
    
    public static String getAllHostAddr() {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface
                    .getNetworkInterfaces();
            InetAddress ip = null;
            StringBuilder ipAddr = new StringBuilder();
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces
                        .nextElement();
                Enumeration<InetAddress> addresses = netInterface
                        .getInetAddresses();
                while (addresses.hasMoreElements()) {
                    ip = addresses.nextElement();
                    if (ip != null && !ip.isLoopbackAddress()
                            && ip instanceof Inet4Address) {
                        ipAddr.append(ip.getHostAddress()).append(";");
                    }
                }
            }
            if (ipAddr.length() == 0) {
                return "unknown host";
            } else {
                return ipAddr.toString();
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            return "unknown host";
        }
    }
    public static String getHostAddr() {
    	try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			log.error(e.getMessage(),e);
		}
    	return "";
    }
    //从请求的referer中获取访问者域名
    public static String getDomainNameFromReferer(HttpServletRequest request){
        String referer=request.getHeader("REFERER");
        if(referer == null) {
            return null;
        }
        String domainName="";
        int beginIndex=referer.indexOf("//")+2;
        int endIndex=referer.indexOf("/",beginIndex);
        domainName=referer.substring(beginIndex, endIndex);
        return domainName;
    }
    //获取浏览器端客户IP地址
    public static String getClientAddr(HttpServletRequest request){
        String ip = request.getHeader("x-forwarded-for");
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("x-real-ip");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return ip;
    }
    
    public static String getProcessId() {  
        String pName = ManagementFactory.getRuntimeMXBean().getName();  
        int index = pName.indexOf('@');  
        if (index == -1) {  
            return ""; 
        }  
        String pid = pName.substring(0, index);  
        return pid;  
    } 
}

