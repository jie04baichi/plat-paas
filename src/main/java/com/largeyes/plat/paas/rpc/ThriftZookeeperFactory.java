package com.largeyes.plat.paas.rpc;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.Logger;

public class ThriftZookeeperFactory {

	private static Logger logger = Logger.getLogger(ThriftZookeeperFactory.class);
	
    private static ThriftZookeeperFactory instance = new ThriftZookeeperFactory();

    private volatile CuratorFramework client;

    private ThriftZookeeperFactory() {
    };

    public static ThriftZookeeperFactory getInstance() {
        return instance;
    }

    public CuratorFramework getZookeeperClient() {
        if (this.client == null) {
            synchronized (this) {
                if (this.client == null) {
                    Builder builder = CuratorFrameworkFactory.builder().connectString(getZookeeperConnectString())
                            .retryPolicy(new RetryNTimes(10, 1000)).connectionTimeoutMs(5000);
                    String namespace = getNamespace();
                    builder.namespace(namespace);

                    client = builder.build();
                    client.start();
                }
            }
        }
        return this.client;
    }


    public String getNamespace() {
        return "thrift";
    }

    public String getZookeeperConnectString() {
        String zookeeperStr = System.getProperty("zookeeper.ips");
        logger.info("********** Zookeeper ip string ********** " + zookeeperStr);
        return zookeeperStr;
    }

}