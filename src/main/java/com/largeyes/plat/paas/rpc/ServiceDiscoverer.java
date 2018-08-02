package com.largeyes.plat.paas.rpc;  

import java.io.IOException;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.DownInstancePolicy;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.strategies.RoundRobinStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * a service discoverer based on curator framework
 * 
 * @author hao
 *
 * @param <T>
 *            service payload class
 */
public class ServiceDiscoverer<T> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected CuratorFramework curator;
	protected String basePath;
	protected Class<T> payloadClass;

	private ServiceDiscovery<T> discover;
	Map<String, ServiceProvider<T>> providers = Maps.newHashMap();

	@SuppressWarnings("rawtypes")
	private static ServiceDiscoverer instance;

	@SuppressWarnings("unchecked")
	public static <T> ServiceDiscoverer<T> getInstance(CuratorFramework curator, String basePath, Class<T> payloadClass) {
		if (instance == null) {
			synchronized (ServiceDiscoverer.class) {
				if (instance == null) {
					instance = new ServiceDiscoverer<T>(curator, basePath, payloadClass);
				}
			}
		}
		return instance;
	}

	public ServiceDiscoverer(CuratorFramework curator, String basePath, Class<T> pc) {
		this.curator = curator;
		this.basePath = basePath;
		this.payloadClass = pc;

		if (curator == null || basePath == null) {
			throw new IllegalArgumentException("the curator or basePath can't be null");
		}

		JsonInstanceSerializer<T> serializer = new JsonInstanceSerializer<T>(payloadClass);
		this.discover = ServiceDiscoveryBuilder.builder(payloadClass).basePath(basePath).client(curator)
				.serializer(serializer).build();
		try {
			this.discover.start();
		} catch (Exception e) {
			throw new IllegalStateException("start discovery error");
		}

	}

	public void destroy() throws IOException {
		this.discover.close();
	}

	public void register(ServiceInstance<T> service) throws Exception {
		// log.info("register service instance:{}", service);
		this.discover.registerService(service);
	}

	public void unregister(ServiceInstance<T> service) throws Exception {
		this.discover.unregisterService(service);
	}

	public ServiceProvider<T> getServiceProvider(String serviceName) throws Exception {
		ServiceProvider<T> provider = providers.get(serviceName);
		if (provider == null) {
			provider = discover.serviceProviderBuilder().serviceName(serviceName)
					.providerStrategy(new RoundRobinStrategy<T>()).downInstancePolicy(new DownInstancePolicy()).build();
			providers.put(serviceName, provider);
			provider.start();
		}
		return provider;
	}

	public ServiceInstance<T> getInstance(String serviceName) throws Exception {

		return getServiceProvider(serviceName).getInstance();
	}

	public void noteError(String serviceName, ServiceInstance<T> serviceInstance) throws Exception {
        getServiceProvider(serviceName).noteError(serviceInstance);
	}
	
	public ServiceDiscovery<T> getServiceDiscovery() {
		return discover;
	}

}

 