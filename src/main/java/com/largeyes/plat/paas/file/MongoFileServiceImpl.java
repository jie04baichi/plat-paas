package com.largeyes.plat.paas.file;


import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;

import com.alibaba.fastjson.JSONObject;
import com.largeyes.plat.paas.mongo.MongoDBClient;
import com.largeyes.plat.paas.rmc.ConfigurationCenter;
import com.largeyes.plat.paas.rmc.DefaultConfigurationWatcher;

public class MongoFileServiceImpl extends DefaultConfigurationWatcher implements IFileService{
    
    private static final Logger log =  Logger.getLogger(MongoFileServiceImpl.class);
    
    private static final String FILE_SERVER_KEY = "fileServer";
    private static final String FILE_DB_KEY = "fileDb";
    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String FILE_DOMAIN_KEY = "filedomain";
    
    private String fileServer = null;
    private String fileDb = "default_file_db";
    private String username = null;
    private String password = null;
    private String fileDomain = null;
    
    private MongoDBClient mongo = null;
    private ConfigurationCenter cc = null;
    
    private String configpath = "/config/plat/paas/file/conf";

    public void init()
    {
        try {
            initMongoConfig(cc.getConfPathAndWatch(configpath, this));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
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
        if(json.getString(FILE_SERVER_KEY) != null && !json.getString(FILE_SERVER_KEY).equals(fileServer)) {
            fileServer = json.getString(FILE_SERVER_KEY);
        }
        if(json.getString(USERNAME_KEY) != null && !json.getString(USERNAME_KEY).equals(username)) {
            username = json.getString(USERNAME_KEY);
        }
        if(json.getString(PASSWORD_KEY) != null && !json.getString(PASSWORD_KEY).equals(password)) {
            password = json.getString(PASSWORD_KEY);
        }
        if(json.getString(FILE_DB_KEY) != null && !json.getString(FILE_DB_KEY).equals(fileDb)) {
            fileDb = json.getString(FILE_DB_KEY);
        }
        if(json.getString(FILE_DOMAIN_KEY) != null && !json.getString(FILE_DOMAIN_KEY).equals(fileDomain)) {
        	fileDomain = json.getString(FILE_DOMAIN_KEY);
        }
        if(fileServer != null) {
            mongo = new MongoDBClient(fileServer,fileDb, username, password);
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
    public String saveFile(String fileName, String fileType) {
        return mongo.saveFile(fileDb, fileName, fileType);
    }

    @Override
    public String saveFile(byte[] byteFile, String fileName, String fileType) {
        return mongo.saveFile(fileDb, byteFile,fileName,fileType);
    }

    @Override
    public byte[] readFile(String fileId) {
        return mongo.readFile(fileDb, fileId);
    }

    @Override
    public void readFile(String fileId, String localFileName) {
        mongo.readFile(fileDb, fileId, localFileName);
    }

    @Override
    public void deleteFile(String fileId) {
        mongo.deleteFile(fileDb, fileId);
    }

    @Override
    public String updateFile(byte[] byteFile, String fileId, String fileName, String fileType)
            throws Exception {
        return mongo.updateFile(fileDb, byteFile, fileId, fileName, fileType);
    }
	@Override
	public String getImageUrl(String fileId) {
		return fileDomain + "/images"+ "/" + fileId + "." + mongo.getContentType(fileDb, fileId);
	}
	@Override
	public String getFilesUrl(String fileId) {
		return fileDomain + "/files"+ "/" + fileId + "." + mongo.getContentType(fileDb, fileId);
	}
}

