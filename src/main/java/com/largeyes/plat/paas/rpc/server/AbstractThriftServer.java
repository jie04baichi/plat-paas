package com.largeyes.plat.paas.rpc.server;  

import java.lang.reflect.Constructor;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;

public abstract class AbstractThriftServer implements BeanNameAware, InitializingBean, SmartLifecycle {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private static final int DEFAULT_PORT = 13900;
	private static final int DEFAULT_WORKER_SIZE = 100;
	private static final int DEFAULT_SELECTOR_SIZE = 2;
	private static final ServerType DEFAULT_SERVER_TYPE = ServerType.threadselected;
	private static final int DEFAULT_CLIENT_TIMEOUT = 0;
	protected ServerType serverType = DEFAULT_SERVER_TYPE;
	protected int port = DEFAULT_PORT;
	protected int workerSize = DEFAULT_WORKER_SIZE;
	protected int selectorSize = DEFAULT_SELECTOR_SIZE;

	protected TServerTransport transport;
	protected TServer server;

	private Object activeMonitor = new Object();
	private volatile boolean active = false;

	protected String serverName = "defaultThriftServer";
	private int clientTimeout = DEFAULT_CLIENT_TIMEOUT;

	public void setServerType(String serverType) {
		this.serverType = ServerType.valueOf(serverType);
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setWorkerSize(int workerSize) {
		this.workerSize = workerSize;
	}

	public int getSelectorSize() {
		return selectorSize;
	}

	public void setSelectorSize(int selectorSize) {
		this.selectorSize = selectorSize;
	}

	public String getServerType() {
		return serverType.name();
	}

	public int getPort() {
		return port;
	}

	public int getWorkerSize() {
		return workerSize;
	}

	public String getServerName() {
		return serverName;
	}

	protected abstract void createServer() throws Exception;

	protected TProcessor createProcessor(Class<TProcessor> processorClass, Class<?> handlerInterface, Object handler)
			throws Exception {
		Constructor<TProcessor> c = processorClass.getConstructor(handlerInterface);
		return c.newInstance(handler);
	}

	protected TProtocolFactory createProtocolFactory() {

		return new TBinaryProtocol.Factory();

	}

	protected void createServerTransport() throws TTransportException {
		switch (serverType) {
		case threadselected:
		case hsha:
		case nonblocking:
			transport = new TNonblockingServerSocket(port, clientTimeout);
			break;
		case threadpool:
		case simple:
			transport = new TServerSocket(port, clientTimeout);
			break;
		default:
			throw new IllegalArgumentException("Unknow server type " + serverType);
		}
	}

	class ThriftServerRunner implements Runnable {
		@Override
		public void run() {
			try {
				server.serve();
			} catch (Exception e) {
				log.error("Fatal Error!! start {} on port {} failed.", serverName, port);
				log.error("Fatal Error!!", e);
			}
		}
	}

	
	
	
	
	@Override
	public void setBeanName(String name) {
		this.serverName = name;
	}

	@Override
	public void start() {
		if (!active) {
			synchronized (activeMonitor) {
				if (!active) {
					log.info("Starting thrift server: {} on port: {}...", serverName, port);
					try {
						createServerTransport();
						createServer();
					} catch (Exception e) {
						log.error("Fatal Error!! start thrift server: {} on port: {} failed.", serverName, port);
						return;
					}
					new Thread(new ThriftServerRunner(), "Thrift Runner: " + getClass().getSimpleName()).start();
					active = true;
					log.info("Start thrift server: {} on port: {} success.", serverName, port);
				} else {
					log.info("Thrift server: {} had already started with port: {} .", serverName, port);
				}
			}
		}
	}

	@Override
	public void stop() {
		if (active) {
			synchronized (activeMonitor) {
				if (active) {
					active = false;
					server.stop();
					log.info("Stop {} on port {} success", serverName, port);
				}
			}
		}
	}

	@Override
	public boolean isRunning() {
		return active;
	}

	@Override
	public int getPhase() {
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		this.stop();
		callback.run();
	}
	 enum ServerType {
		    nonblocking, hsha, threadselected, threadpool, simple
		}
}