package com.largeyes.plat.paas.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.largeyes.plat.paas.redis.RedisClient;
import com.largeyes.plat.paas.redis.RedisConfig;
import com.largeyes.plat.paas.rmc.ConfigurationCenter;
import com.largeyes.plat.paas.rmc.DefaultConfigurationWatcher;

public class RedisCacheImpl extends DefaultConfigurationWatcher implements IRemoteCache {

	private static final Logger log = Logger.getLogger(RedisCacheImpl.class);

	private ConfigurationCenter cc = null;
	private RedisClient redisCache = null;
	
	private String configpath = RedisConfig.CONFIG_PATH;

	private int dbIndex = 0;
	
	public void init()
	{
		try {
			initCacheConfig(cc.getConfPathAndWatch(configpath, this));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			log.error(e.getMessage(), e);
		}
	}
	@Override
	public void process(WatchedEvent event) {	
		
		if(event.getType().NodeDataChanged.equals(event.getType()))
		{
			try {
	            initCacheConfig(cc.getConfPathAndWatch(configpath, this));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				log.error(e.getMessage(), e);
			}
		}
	}
	
	public void initCacheConfig(String cacheconfig)
	{
        if(log.isInfoEnabled()) {
            log.info("new cache configuration is received: " + cacheconfig);
        }
        
		JSONObject json = (JSONObject) JSON.parse(cacheconfig);
		
		if(json.getString(RedisConfig.DBINDEX_KEY) != null && !json.getString(RedisConfig.DBINDEX_KEY).equals(dbIndex)) {
			dbIndex = json.getIntValue(RedisConfig.DBINDEX_KEY);
		}
		
		redisCache = new RedisClient(cacheconfig);
		
        if(log.isInfoEnabled()) {
            log.info("cache configuration is change: " + cacheconfig);
        }
	}
	
	
	
	
	public ConfigurationCenter getCc() {
        return cc;
    }
    public void setCc(ConfigurationCenter cc) {
        this.cc = cc;
    }
    public String getConfigpath() {
        return configpath;
    }
    public void setConfigpath(String configpath) {
        this.configpath = configpath;
    }
    public int getDbIndex() {
        return dbIndex;
    }
    public void setDbIndex(int dbIndex) {
        this.dbIndex = dbIndex;
    }

	@Override
	public void deleteItem(String key) {
		redisCache.deleteItem(dbIndex, key);
	}
	@Override
    public void addItemToList(String key, Object object) {
        redisCache.addItemToList(dbIndex,key, object);
    }
	@Override
    @SuppressWarnings("rawtypes")
    public List getItemFromList(String key) {
        return redisCache.getItemFromList(dbIndex,key);
    }
	@Override
    public void addItem(String key, Object object) {
        redisCache.addItem(dbIndex,key, object);

    }
    
    public void addItem(String key, Object object, int seconds) {
        redisCache.addItem(dbIndex,key, object, seconds);
    }

    public Object getItem(String key) {
        return redisCache.getItem(dbIndex,key);
    }

    public long getIncrement(String key) {
        return redisCache.getIncrement(dbIndex,key);
    }
    public long getDecrement(String key) {
        return redisCache.getDecrement(dbIndex,key);
    }

    public void setHashMap(String key, HashMap<String, String> map) {
        redisCache.setHashMap(dbIndex,key, map);
    }

    public Map<String, String> getHashMap(String key) {
        return redisCache.getHashMap(dbIndex,key);
    }

    public void addSet(String key, Set<String> set) {
        redisCache.addSet(dbIndex,key, set);
    }

    public Set<String> getSet(String key) {
        return redisCache.getSet(dbIndex,key);
    }
    
    public boolean exists(String key) {
        return redisCache.exists(dbIndex,key);
    }
    
    @SuppressWarnings("rawtypes")
    public List keys(String keyPattern) {
        return redisCache.keys(dbIndex, keyPattern);
    }
    
    public void hsetItem(String key, String field, Object object) {
        redisCache.hsetItem(dbIndex, key, field, object);
    }
    
    public Object hgetItem(String key, String field) {
        return redisCache.hgetItem(dbIndex, key, field);
    }
    
    public void hdelItem(String key, String field) {
        redisCache.hdelItem(dbIndex, key, field);
    }
    
    public void expire(String key, int seconds) {
        redisCache.expire(dbIndex, key, seconds);
    }
    
    public Set<String> hkeys(String key) {
        return redisCache.hkeys(dbIndex, key);
    }
    
    @Override
    public void addItemInt(String key, Long object) {
        
        redisCache.addItemInt(dbIndex, key, object);
    }

    @Override
    public void saddItem(String key, Object object) {
        redisCache.saddItem(dbIndex, key, object);
    }
    
    @Override
    public void addItemFile(String key, byte[] file) {
        redisCache.addItemFile(key, file);
    }

    
    @Override
    public void zaddItem(String key, String item, double score) {
        redisCache.zaddItem(dbIndex, key, item, score);
    }

    @Override
    public Set<String> zgetItems(String key) {
        return redisCache.zgetItems(dbIndex, key);
    }
}
