package com.largeyes.plat.paas.redis;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.largeyes.plat.paas.utils.SerializeUtil;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class RedisClient {
	private static final Logger log = Logger.getLogger(RedisClient.class);

	private ShardedJedisPool pool = null;
	private GenericObjectPoolConfig config = null;
	
	public RedisClient(String cacheconfig)
	{
		 JSONObject json = (JSONObject) JSON.parse(cacheconfig);

			if (json != null) {

				config = new GenericObjectPoolConfig();
				//config.setMaxActive(json.getIntValue(RedisConfig.MAXACTIVE_KEY));
				config.setMaxIdle(json.getIntValue(RedisConfig.MAXIDLE_KEY));
				//config.setMaxWait(json.getLong(RedisConfig.MAXWAIT_KEY));
				config.setTestOnBorrow(json.getBoolean(RedisConfig.TESTONBORROW_KEY));
				config.setTestOnReturn(json.getBoolean(RedisConfig.TESTONRETURN_KEY));
				config.setTimeBetweenEvictionRunsMillis(json.getLong(RedisConfig.TIMEBETWEENEVICTIONRUNSMILLIS_KEY));
				config.setNumTestsPerEvictionRun(json.getIntValue(RedisConfig.NUMTESTSPEREVICTIONRUN_KEY));
				config.setMinEvictableIdleTimeMillis(json.getLong(RedisConfig.MINEVICTABLEIDLETIMEMILLIS_KEY));
				config.setTestWhileIdle(json.getBoolean(RedisConfig.TESTWHILEIDLE_KEY));
				config.setSoftMinEvictableIdleTimeMillis(json.getLong(RedisConfig.SOFTMINEVICTABLEIDLETIMEMILLIS_KEY));
				
				List<InetSocketAddress> addrs = getAddresses(json.getString(RedisConfig.HOSTS_KEY));
				
		        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		        for (InetSocketAddress addr : addrs) {
		            JedisShardInfo shard = new JedisShardInfo(addr.getAddress().getHostAddress(), addr.getPort());
		            shards.add(shard);
		        }
		        pool = new ShardedJedisPool(config, shards);
			}
	}

	private List<InetSocketAddress> getAddresses(String hosts){
        if (hosts == null) {
            throw new NullPointerException("Null host list");
        }
        if (hosts.trim().equals("")) {
            throw new IllegalArgumentException("No hosts in list:  ``" + hosts + "''");
        }
        ArrayList<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();

        for (String hoststuff : hosts.split(",")) {
            int finalColon = hoststuff.lastIndexOf(':');
            if (finalColon < 1) {
                throw new IllegalArgumentException("Invalid server ``" + hoststuff + "'' in list:  " + hosts);
            }
            String hostPart = hoststuff.substring(0, finalColon);
            String portNum = hoststuff.substring(finalColon + 1);

            addrs.add(new InetSocketAddress(hostPart, Integer.parseInt(portNum)));
        }
        assert !addrs.isEmpty() : "No addrs found";
        return addrs;
    }
	
    public void addItemToList(int dbIndex, String key, Object object) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.lpush(key.getBytes(), SerializeUtil.serialize(object));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List getItemFromList(int dbIndex, String key) {
        ShardedJedis jedis = null;
        List<byte[]> ss = null;
        List data = new ArrayList();
        try {
            jedis = pool.getResource();
            long len = jedis.llen(key);
            if (len == 0)
                return null;
            ss = jedis.lrange(key.getBytes(), 0, (int) len - 1);
            for (int i = 0; i < len; i++) {
                data.add(SerializeUtil.deserialize(ss.get(i)));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
        return data;

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List keys(int dbIndex, String keyPattern) {
        ShardedJedis jedis = null;
        Set<byte[]> ss = null;
        List data = new ArrayList();
        try {
            jedis = pool.getResource();
            ss = jedis.hkeys(keyPattern.getBytes());
            for (Iterator<byte[]> iterator = ss.iterator(); iterator.hasNext();) {
                data.add(new String(iterator.next()));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
        return data;

    }

    public void addItem(int dbIndex, String key, Object object) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.set(key.getBytes(), SerializeUtil.serialize(object));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

    }
/*
    public String flushDB(int dbIndex) {
        ShardedJedis jedis = null;
        String result = "";
        try {
            jedis = pool.getResource();
            jedis.select(dbIndex);
            result = jedis.flushDB();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            pool.returnBrokenResource(jedis);
        } finally {
            if (jedis != null)
                pool.returnResource(jedis);
        }
        return result;
    }
    */

    public boolean exists(int dbIndex, String key) {
        ShardedJedis jedis = null;
        boolean result = false;
        try {
            jedis = pool.getResource();
            result = jedis.exists(key.getBytes());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
        return result;
    }

    public void addItem(int dbIndex, String key, Object object, int seconds) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.setex(key.getBytes(), seconds,
                    SerializeUtil.serialize(object));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
    }
    
    public void addItemInt(int dbIndex,String key,Long object){
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.set(key, String.valueOf(object));
        } catch (Exception e) {
            log.error("exception:" + e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
    }

    public Object getItem(int dbIndex, String key) {
        ShardedJedis jedis = null;
        byte[] data = null;
        try {
            jedis = pool.getResource();
            data = jedis.get(key.getBytes());
            return SerializeUtil.deserialize(data);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

    }

    public void deleteItem(int dbIndex, String key) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.del(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

    }
    
    public void hsetItem(int dbIndex, String key, String field, Object object) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.hset(key.getBytes(), field.getBytes(),
                    SerializeUtil.serialize(object));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

    }
    
    public Object hgetItem(int dbIndex, String key, String field) {
        ShardedJedis jedis = null;
        byte[] data = null;
        try {
            jedis = pool.getResource();
            data = jedis.hget(key.getBytes(), field.getBytes());
            return SerializeUtil.deserialize(data);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

    }
    
    public void hdelItem(int dbIndex, String key, String field) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.hdel(key.getBytes(), field.getBytes());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

    }

    public Set<String> hkeys(int dbIndex, String key) {
        ShardedJedis jedis = null;
        Set<String> data = null;
        try {
            jedis = pool.getResource();
            data = jedis.hkeys(key);
            return data;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

    }

    public void expire(int dbIndex, String key, int seconds) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.expire(key.getBytes(), seconds);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
    }

    public long getIncrement(int dbIndex, String key) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.incr(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return 0L;
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

    }

    public long getDecrement(int dbIndex, String key) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            return jedis.decr(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return 0L;
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

    }

    public void setHashMap(int dbIndex, String key, HashMap<String, String> map) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            if (map != null && !map.isEmpty()) {
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    jedis.hset(key, entry.getKey(), entry.getValue());
                }

            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

    }

    public Map<String, String> getHashMap(int dbIndex, String key) {
        Map<String, String> map = new HashMap<String, String>();
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            map = jedis.hgetAll(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
        return map;

    }

    public void addSet(int dbIndex, String key, Set<String> set) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            if (set != null && !set.isEmpty()) {
                for (String value : set) {
                    jedis.sadd(key, value);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
    }

    public Set<String> getSet(int dbIndex, String key) {
        Set<String> sets = new HashSet<String>();
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            sets = jedis.smembers(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

        return sets;
    }
    
    public void saddItem(int dbIndex, String key ,Object object){
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.sadd(key.getBytes(), SerializeUtil.serialize(object));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
    }
    
    public void addItemFile(String key, byte[] file) {
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.set(key.getBytes(), file);
        } catch (Exception e) {
            log.error("",e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }

    }
    
    public void zaddItem(int dbIndex, String key ,String item, double score){
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            jedis.zadd(key, score, item);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
    }
    
    public Set<String> zgetItems(int dbIndex, String key){
        Set<String> sets = new TreeSet<String>();
        ShardedJedis jedis = null;
        try {
            jedis = pool.getResource();
            sets = jedis.zrange(key, 0, -1);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (jedis != null){
            	jedis.close();
            }
        }
        return sets;
    }
	
}
