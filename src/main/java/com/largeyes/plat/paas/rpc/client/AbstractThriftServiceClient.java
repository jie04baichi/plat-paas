package com.largeyes.plat.paas.rpc.client;  

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;



public abstract class  AbstractThriftServiceClient implements FactoryBean<Object> {

	protected static final Logger logger = LoggerFactory.getLogger(AbstractThriftServiceClient.class);

	private Class<?> thriftIfaceClass;

	private Class<? extends TServiceClient> thriftClientClass;

	@Override
	public Object getObject() throws Exception {
		//serviceProvideer = getServiceProvider();
		return Enhancer.create(thriftIfaceClass, new ClientInvokerHander());
	}

	protected abstract TTransport getTransport() throws Exception;
	

	protected String getServiceName(){
		String serviceClassName = thriftClientClass.getName().replace("$Client", "");
		return serviceClassName.substring(serviceClassName.lastIndexOf(".") + 1);
	}

	@Override
	public Class<?> getObjectType() {
		return thriftIfaceClass;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	
	class ClientInvokerHander implements MethodInterceptor {

		@Override
		public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
			String serviceName = getServiceName();

			TTransport transport = null;
			try {

				transport = getTransport();
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);

				Constructor<? extends TServiceClient> clientConstructor = thriftClientClass.getConstructor(new Class<?>[]{TProtocol.class});
				TServiceClient client = clientConstructor.newInstance(protocol);

				Method targetMethod = thriftClientClass.getMethod(method.getName(), method.getParameterTypes());
				Object result = targetMethod.invoke(client, args);
				return result;

			} catch (InvocationTargetException e) {
				Throwable targetException = e.getTargetException();
				if(targetException instanceof TApplicationException) {
					TApplicationException ex = (TApplicationException) e.getTargetException();
					if(ex.getType() == TApplicationException.MISSING_RESULT) {
						return null;
					}
				}
                
				String errorMsg = String.format("invoke_error service=%s, error=%s", serviceName, targetException.getMessage());
				logger.error(errorMsg,targetException);
				throw targetException;
			}catch (Exception e) {
				String errorMsg = String.format("invoke_error service=%s, error=%s", serviceName, e.getMessage());
				logger.error(errorMsg,e);
				throw e;
			}finally{
				if(transport != null){
					transport.close();
				}
			}
		}
	}

	public void setThriftIfaceClass(Class<?> thriftIfaceClass) {
		this.thriftIfaceClass = thriftIfaceClass;
	}

	public void setThriftClientClass(Class<? extends TServiceClient> thriftClientClass) {
		this.thriftClientClass = thriftClientClass;
	}

}