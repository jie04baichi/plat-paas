package com.largeyes.plat.paas.utils;

import java.util.List;

import com.largeyes.plat.paas.cache.IRemoteCache;

public class CacheUtil {
    private static IRemoteCache remoteCache;
    static{
        remoteCache = PaasUtilsContextHolder.getBean("remoteCache", IRemoteCache.class);
    }
    
    public static void addItem(String key, Object object){
        remoteCache.addItem(key, object);
    }
    public static Object getItem(String key){
        return remoteCache.getItem(key);
    }
    public static void deleteItem(String key){
        remoteCache.deleteItem(key);
    }
    public static void addItemToList(String key, Object object){
    	remoteCache.addItemToList(key, object);
    	
    }
    public static List getItemFromList(String key){
    	return remoteCache.getItemFromList(key);
    	
    }
}

