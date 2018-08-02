package com.largeyes.plat.paas.rpc;

import static java.lang.String.format;

import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.curator.x.discovery.ServiceInstance;

import com.largeyes.plat.paas.utils.NetworkUtil;
import com.sohu.pp.common.discovery.ServiceDiscoverer;
import com.sohu.pp.common.thrift.ThriftServiceMetadata;
import com.sohu.pp.common.thrift.server.ThriftServer;

public class DiscoveredThriftServer extends ThriftServer {

	private ServiceDiscoverer<ThriftServiceMetadata> serviceDiscoverer;

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		serviceDiscoverer = ServiceDiscoverer.getInstance(
				ThriftZookeeperFactory.getInstance().getZookeeperClient(), "/pp/serviceDiscovery",
				ThriftServiceMetadata.class);
	}

	@Override
	public void start() {
		super.start();
		try {
			ServiceInstance<ThriftServiceMetadata> serviceInstance = getServiceInstance();
			serviceDiscoverer.register(serviceInstance);
			if (log.isInfoEnabled()) {
				log.info(String.format("service %s register success!", serviceInstance));
			}
		} catch (Exception e) {
			log.error("error occured when register service {}, {}", getServerName(), e);
		}
	}

	@Override
	public void stop() {
		super.stop();
		try {
			serviceDiscoverer.unregister(getServiceInstance());
			serviceDiscoverer.destroy();
		} catch (Exception e) {
			log.error("error occured when unregister service {}", getServerName());
		}
	}

	private ServiceInstance<ThriftServiceMetadata> getServiceInstance() throws Exception {
		return ServiceInstance.<ThriftServiceMetadata> builder().id(getServiceId()).name(getServerName())
				.address(NetworkUtil.getHostAddr()).port(port).payload(new ThriftServiceMetadata("2.5")).build();
	}



	private String getServiceId() {
		try {
			String url = String.format("thrift://%s:%s", NetworkUtil.getHostAddr(), port);
			return URLEncoder.encode(url, "UTF-8");
		} catch (Exception e) {
			return generateServiceId();
		}
	}

	private String generateServiceId() {
		UUID uuid = UUID.randomUUID();
		try {
			return format("%s-%d-%s", InetAddress.getLocalHost().getHostName(), //
					System.currentTimeMillis(), //
					Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8));
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException("can not generate service id by auto");
		}
	}

}
