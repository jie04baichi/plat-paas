package com.largeyes.plat.paas.rmc;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

public class ConfigurationCenter {
	private static final Logger log = Logger.getLogger(ConfigurationCenter.class);
	
	public static final String DEV_MODE="D";
	public static final String PROD_MODE="P";
	
	private ZooKeeper zookeeper = null;	
	private String ConnectIP = null;
	private String zkAuth = null;	
	private String runMode = DEV_MODE;//P:product mode; D:dev mode
	private String charSet = null;
	
	private int SESSION_TIME_OUT = 10000;//zk session time out ms
	
	private String configurationFile="PaasConfigurationFile.properties";// runMode properties file
	
	private Properties props = new Properties(); 
	
	public ConfigurationCenter(){
		this.ConnectIP = System.getProperty("zookeeper.ips");
		this.runMode = System.getProperty("paas.run.mode");
	}
	
	public ConfigurationCenter(String zkConnectIP, String runMode)
	{
		this.ConnectIP = zkConnectIP;
		this.runMode = runMode;	
	}
	
	public ConfigurationCenter(String zkConnectIP, String runMode, String charSet)
	{
		this.ConnectIP = zkConnectIP;
		this.runMode = runMode;	
		this.charSet = charSet;
	}
	
	public ConfigurationCenter(String zkConnectIP, String runMode, String charSet, String configurationFile)
	{
		this.ConnectIP = zkConnectIP;
		this.runMode = runMode;
		this.charSet = charSet;
		if(configurationFile != null)
		{
			this.configurationFile = configurationFile;
		}
	}
	
	public void init()
	{
		System.setProperty("zookeeper.ips", ConnectIP);
		System.setProperty("paas.run.mode", runMode);
		
		//1.DEV_MODE
		if(DEV_MODE.equals(runMode))
		{
			try {
				//props.load(ConfigurationCenter.class.getClassLoader().getSystemResourceAsStream(configurationFile));
				props = loadAllProperties(configurationFile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				log.error(e.getMessage(), e);
			}
			return;
		}
		
		try {
			zookeeper = new ZooKeeper(ConnectIP, SESSION_TIME_OUT, new DefaultConfigurationWatcher());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			log.error(e.getMessage(), e);
		}
	}
	
	public String getConfPathAndWatch(String confPath, DefaultConfigurationWatcher watcher) throws Exception
	{
		
		try {
			if(DEV_MODE.equals(runMode))
			{
				return props.getProperty(confPath);
			}
			else {
				if(charSet == null)
				{
					return new String(zookeeper.getData(confPath, watcher, null));
				}
				else {
					return new String(zookeeper.getData(confPath, watcher, null), charSet);
				}
			}		
	} catch (Exception e) {
		// TODO: handle exception
		log.error(e.getMessage(), e);
		throw e;
	}
}

	public String getConfPath(String confPath) throws Exception
	{
		try {
				if(DEV_MODE.equals(runMode))
				{
					return props.getProperty(confPath);
				}
				else {
					if(charSet == null)
					{
						return new String(zookeeper.getData(confPath, true, null));
					}
					else {
						return new String(zookeeper.getData(confPath, true, null), charSet);
					}
				}		
		} catch (Exception e) {
			// TODO: handle exception
			log.error(e.getMessage(), e);
			throw e;
		}
	}
    public String getZkAuth() {
        return zkAuth;
    }

    public void setZkAuth(String zkAuth) {
        this.zkAuth = zkAuth;
    }
    private Properties loadAllProperties(String resourceName) throws IOException {
        ClassLoader clToUse = ConfigurationCenter.class.getClassLoader();
        Properties properties = new Properties();
        Enumeration<URL> urls = clToUse.getResources(resourceName);
        while (urls.hasMoreElements()) {
            URL url = urls.nextElement();
            InputStream is = null;
            try {
                URLConnection con = url.openConnection();
                con.setUseCaches(false);
                is = con.getInputStream();
                properties.load(is);
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        }
        return properties;
    }
}
