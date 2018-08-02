package com.largeyes.plat.paas.mongo;

import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**  
 *  
 * @author pengjie  
 */
public interface IRemoteMongoDB {
	
	public void insert(String dbName, String collectionName, String doc);
	public void remove(String dbName, String collectionName, String doc);
	public <T> List<T>  find(String dbName, String collectionName, JSONObject cond,  Class<T> clazz);
}
 