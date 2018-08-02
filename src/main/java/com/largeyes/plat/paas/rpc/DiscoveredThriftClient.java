package com.largeyes.plat.paas.rpc;


import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.largeyes.plat.paas.rpc.client.AbstractThriftServiceClient;
import com.largeyes.plat.paas.rpc.server.ThriftServiceMetadata;

public class DiscoveredThriftClient extends AbstractThriftServiceClient {

	private static boolean isLocalDebug = false;
	
	static{
		isLocalDebug = "true".equals(System.getProperty("config.local.debug"))
				|| "true".equals(System.getProperty("config.local"));
		if(isLocalDebug){
			logger.debug("############ enable localhost debug mode for thrift service invoke. ############");
		}
	}
	
	private ServiceDiscoverer<ThriftServiceMetadata> discovery;

	public DiscoveredThriftClient() throws Exception {
		discovery = ServiceDiscoverer.getInstance(ThriftZookeeperFactory.getInstance()
				.getZookeeperClient(), "/pp/serviceDiscovery", ThriftServiceMetadata.class);
	}

	@Override
	protected TTransport getTransport() throws Exception  {
		ServiceInstance<ThriftServiceMetadata> serviceInstance = null;
		try {
			serviceInstance = discovery.getInstance(getServiceName());
			if (serviceInstance == null) {
				throw new RuntimeException(String.format("invoke service %s error, can't find any service provider!",
						getServiceName()));
			}
		} catch (Exception e) {
			logger.info("调用 service 失败 ip:" + serviceInstance.getAddress() + ", port:" + serviceInstance.getPort(), e);
			throw new Exception(e);	
		}

		String host = serviceInstance.getAddress();
		if(isLocalDebug){
			host = "localhost";
		}
		
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("invoke_service %s --> %s:%s ", getServiceName(), host,
					serviceInstance.getPort()));
		}
		logger.info(String.format("invoke_service %s --> %s:%s ", getServiceName(), host,serviceInstance.getPort()));
		TTransport socketTransport = new TSocket(host, serviceInstance.getPort());


		return new TFramedTransport(socketTransport);

	}

}
