package com.largeyes.plat.paas.rpc.server;  

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import com.largeyes.plat.paas.rpc.ThriftUtils;




public class ThriftServer extends AbstractThriftServer {
    protected Object handler;
    private Class<?> handlerInterface;
    private Class<TProcessor> processorClass;

    public void setHandler(Object handler) {
        this.handler = handler;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.handler == null) {
            throw new IllegalArgumentException("hander class is null or empty!");
        }
        Class<?> handlerClass = this.handler.getClass();
        Class<?> ifaceClazz = ThriftUtils.getIfaceClass(handlerClass);
        Class<?> serviceClazz = ThriftUtils.getServiceClass(ifaceClazz);
        if (this.handlerInterface == null) {
            if (ifaceClazz == null) {
                throw new IllegalArgumentException(
                    "handler interface is null and the system can't resovle it");
            }
            this.handlerInterface = ifaceClazz;
        }
        if (this.processorClass == null) {
            if (serviceClazz == null) {
                throw new IllegalArgumentException(
                    "Service class is null and the system can't resovle it");
            }
            this.processorClass = ThriftUtils.getProcessorClass(serviceClazz);
            if (this.processorClass == null) {
                throw new IllegalArgumentException(
                    "Processor class is null and the system can't resovle it");
            }
        }
    }

    protected void createServer() throws Exception {
        TProcessor processor = createProcessor(this.processorClass, this.handlerInterface,
            this.handler);
        TProtocolFactory factory = createProtocolFactory();
        switch (serverType) {
            case threadselected:
                org.apache.thrift.server.TThreadedSelectorServer.Args threadSelectedArgs = new org.apache.thrift.server.TThreadedSelectorServer.Args(
                    (TNonblockingServerTransport) transport);
                threadSelectedArgs.protocolFactory(factory);
                threadSelectedArgs.processor(processor);
                threadSelectedArgs.workerThreads(workerSize);
                threadSelectedArgs.selectorThreads(selectorSize);
                TTransportFactory threadSelectedTransportFactory = new TFramedTransport.Factory();
                threadSelectedArgs.transportFactory(threadSelectedTransportFactory);
                server = new TThreadedSelectorServer(threadSelectedArgs);
                break;
            case hsha:
                org.apache.thrift.server.THsHaServer.Args hshaArgs = new org.apache.thrift.server.THsHaServer.Args(
                    (TNonblockingServerTransport) transport);
                hshaArgs.protocolFactory(factory);
                hshaArgs.processor(processor);
                hshaArgs.workerThreads(workerSize);
                TTransportFactory hshaTransportFactory = new TFramedTransport.Factory();
                hshaArgs.transportFactory(hshaTransportFactory);
                server = new THsHaServer(hshaArgs);
                break;
            case nonblocking:
                org.apache.thrift.server.TNonblockingServer.Args nonblockingArgs = new org.apache.thrift.server.TNonblockingServer.Args(
                    (TNonblockingServerTransport) transport);
                nonblockingArgs.protocolFactory(factory);
                nonblockingArgs.processor(processor);
                TTransportFactory nonblockingTransportFactory = new TFramedTransport.Factory();
                nonblockingArgs.transportFactory(nonblockingTransportFactory);
                server = new TNonblockingServer(nonblockingArgs);
                break;
            case threadpool:
                org.apache.thrift.server.TThreadPoolServer.Args threadPoolArgs = new org.apache.thrift.server.TThreadPoolServer.Args(
                    transport);
                threadPoolArgs.protocolFactory(factory);
                threadPoolArgs.processor(processor);
                threadPoolArgs.maxWorkerThreads(workerSize);
                threadPoolArgs.minWorkerThreads(workerSize);
                server = new TThreadPoolServer(threadPoolArgs);
                break;
            case simple:
                org.apache.thrift.server.TSimpleServer.Args simpleArgs = new org.apache.thrift.server.TSimpleServer.Args(
                    transport);
                simpleArgs.protocolFactory(factory);
                simpleArgs.processor(processor);
                server = new TSimpleServer(simpleArgs);
                break;
            default:
                throw new IllegalArgumentException("Unknow server type " + serverType);
        }
    }
}