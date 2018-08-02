package com.largeyes.plat.paas.mongo;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.largeyes.plat.paas.rmc.ConfigurationCenter;
import com.largeyes.plat.paas.rmc.DefaultConfigurationWatcher;
import com.mongodb.DBCursor;

/**  
 *  
 * @author pengjie  
 */
public class RemoteMongoDBImpl  extends DefaultConfigurationWatcher implements IRemoteMongoDB{
	private static final Logger log = Logger.getLogger(RemoteMongoDBImpl.class);

    private MongoDBClient mongo = null;
    private ConfigurationCenter cc = null;

    private static final String SERVER_HOSTS = "server.hosts";
    
    private String configpath = "/config/plat/paas/mongo/conf";
    
    private String serverHosts = null;
    
    public void init()
    {
        try {
            initMongoConfig(cc.getConfPathAndWatch(configpath, this));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @SuppressWarnings("static-access")
	@Override
    public void process(WatchedEvent event) {   
        if(event.getType().NodeDataChanged.equals(event.getType())){
            try {
                initMongoConfig(cc.getConfPathAndWatch(configpath, this));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public void initMongoConfig(String mongoConfig)
    {
        if(log.isInfoEnabled()) {
            log.info("new file service configuration is received: " + mongoConfig);
        }
        JSONObject json = JSONObject.parseObject(mongoConfig);
        if(json.getString(SERVER_HOSTS) != null && !json.getString(SERVER_HOSTS).equals(serverHosts)) {
        	serverHosts = json.getString(SERVER_HOSTS);
        }
        if(serverHosts != null) {
            mongo = new MongoDBClient(serverHosts);
            if(log.isInfoEnabled()) {
                log.info("file server address is changed to " + mongoConfig);
            }
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
	
	@Override
	public void insert(String dbName, String collectionName, String doc) {
		mongo.insert(dbName, collectionName, doc);
	}

	@Override
	public <T> List<T>  find(String dbName, String collectionName, JSONObject cond,  Class<T> clazz) {
		DBCursor cursor = mongo.find(dbName, collectionName, cond);
		
		if (cursor.hasNext()) {
			List<T> lists = JSON.parseArray(cursor.toArray().toString(), clazz);
			return lists;
		}
		return null;
	}

	@Override
	public void remove(String dbName, String collectionName, String doc) {
		mongo.remove(dbName, collectionName, doc);
	}	
}
 