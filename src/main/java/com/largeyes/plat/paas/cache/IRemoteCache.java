package com.largeyes.plat.paas.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.JedisPubSub;

public interface IRemoteCache {
    public void addItemToList(String key, Object object);
    public List getItemFromList(String key);
    public void addItem(String key, Object object);
    public void addItem(String key, Object object, int seconds);
    public void addItemInt(String key,Long object);
    public boolean exists(String key);
    public List keys(String keyPattern);
    public Object getItem(String key);
    public void deleteItem(String key);
    public long getIncrement(String key);
    public long getDecrement(String key);
    public void setHashMap(String key, HashMap<String, String> map);
    public Map<String, String> getHashMap(String key);
    public void addSet(String key, Set<String> set);
    public Set<String> getSet(String key);
    public void hsetItem(String key, String field,Object object);
    public Object hgetItem(String key, String field);
    public void hdelItem(String key, String field);
    public void expire(String key,int seconds);
    public Set<String> hkeys(String key);
    public void saddItem(String key, Object object);
    public void addItemFile(String key, byte[] file);
    public void zaddItem(String key, String item, double score);
    public Set<String> zgetItems(String key);
}
