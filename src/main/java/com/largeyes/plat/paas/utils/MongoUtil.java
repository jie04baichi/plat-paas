package com.largeyes.plat.paas.utils;

import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.largeyes.plat.paas.mongo.IRemoteMongoDB;

/**  
 *  
 * @author pengjie  
 */
public class MongoUtil {
	private static IRemoteMongoDB mongoManager;
	
	static{
		mongoManager = PaasUtilsContextHolder.getBean("mongoManager", IRemoteMongoDB.class);
	}
	
	public static void insert(String dbName, String collectionName, String doc){
		mongoManager.insert(dbName, collectionName, doc);
	}
	public static  <T> List<T>  find(String dbName, String collectionName, JSONObject cond,  Class<T> clazz) {
		return mongoManager.find(dbName, collectionName, cond, clazz);
	}
	public static void remove(String dbName, String collectionName, String doc){
		mongoManager.remove(dbName, collectionName, doc);
	}
}
 