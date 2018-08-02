package com.largeyes.plat.paas.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;

import com.alibaba.fastjson.JSONObject;
import com.largeyes.plat.paas.rmc.ConfigurationCenter;
import com.largeyes.plat.paas.rmc.DefaultConfigurationWatcher;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;

/**
 * 
 * Title: ECP <br>
 * Project Name:PlatPaas <br>
 * Description: <br>
 * Date:2016-1-6下午9:16:06  <br>
 * Copyright (c) 2016 asia All Rights Reserved <br>
 * 
 * @author PJieWin
 * @version  
 * @since JDK 1.6
 * 
 * kafka消息消费类
 */
public class MessageConcumer extends DefaultConfigurationWatcher{
    private String topic;  
    private String groupId;
    private int partitionsNum;  
    
    private ConsumerConnector connector;  
    private ExecutorService threadPool;  
    
    private MessageExecutor executor;
    
    private Properties props = null;

    private ConfigurationCenter cc;
    private String configpath = "/config/plat/paas/kafka/comsumer";
    
    public static final Logger log = Logger.getLogger(MessageConcumer.class);

    public MessageConcumer()
    {
        
    }
    public MessageConcumer(ConfigurationCenter cc, MessageExecutor executor, String configpath, String groupId, String topic, int partitionsNum)throws Exception
    {
        this.cc = cc;
        this.executor = executor;
        this.configpath = configpath;
        this.groupId = groupId;
        this.topic = topic;
        this.partitionsNum = partitionsNum;
    }
    public void init()
    {
        if (log.isInfoEnabled()) {
            log.info("init MessageConcumer...");
        }
        try {
            initProducerConfig(cc.getConfPathAndWatch(configpath, this));
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage(), e);
        }
    }
    @SuppressWarnings("static-access")
    @Override
    public void process(WatchedEvent event) {
        if(event.getType().NodeDataChanged.equals(event.getType()))
        {
            try {
                initProducerConfig(cc.getConfPathAndWatch(configpath, this));
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                log.error(e.getMessage(), e);
            }
        }
    }
    public void initProducerConfig(String producerConfig){
        if (log.isInfoEnabled()) {
            log.info("new MessageConcumer configuration is received: " + producerConfig);
        }
        JSONObject json = JSONObject.parseObject(producerConfig);
        @SuppressWarnings("rawtypes")
        Set<String> keys = json.keySet();
        boolean changed = false;
        if (keys != null) {
            for (String key:keys) {
                if (props == null) {
                    props = new Properties();
                    props.put("group.id", groupId);
                    changed = true;
                }
                if (props.containsKey(key)) {
                    if (props.get(key) == null
                            || !props.get(key).equals(json.getString(key))) {
                        props.put(key, json.getString(key));
                        changed = true;
                    }
                } else {
                    props.put(key, json.getString(key));
                    changed = true;
                }
            }
        }
        if (changed) {
            
            this.stop();

            ConsumerConfig config = new ConsumerConfig(props);
            connector = (ConsumerConnector) Consumer.createJavaConsumerConnector(config); 
            
            this.start();
        }
    }
    
    public void start(){  
        
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put(topic, partitionsNum);  
        Map<String, List<KafkaStream<String,Message>>> topicMessageStreams = connector.createMessageStreams(map, new StringDecoder(null), new MessageDecoder(null));        
        List<KafkaStream<String, Message>> partitions = topicMessageStreams.get(topic); 
        threadPool = Executors.newFixedThreadPool(partitions.size());  
        for(KafkaStream<String, Message> partition : partitions){  
            threadPool.execute(new MessageRunner(partition, executor));  
        }   
    } 
    /*
    public void startMessageConsumer(String topic, MessageExecutor executor)
    {
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put(topic, partitionsNum);
        Map<String, List<KafkaStream<String,Message>>> topicMessageStreams = connector.createMessageStreams(map, 
                new StringDecoder(null), new MessageDecoder(null));        
        List<KafkaStream<String, Message>> partitions = topicMessageStreams.get(topic);  
        threadPool = Executors.newFixedThreadPool(partitions.size());  
        for(KafkaStream<String, Message> partition : partitions){  
            threadPool.execute(new MessageRunner(partition, executor));  
        }  
    }
    public void stopMessageConsumer(String topic)
    {
         stop();
    }
    */
    public void stop()
    {
        if(threadPool == null)
        {
            return;
        }
        try{  
            threadPool.shutdownNow();  
            try {
                while(!threadPool.awaitTermination(1, TimeUnit.SECONDS)) {
                    if(log.isInfoEnabled()) {
                        log.info("old executor is not closed: " + threadPool);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }catch(Exception e){  
            //  
            e.printStackTrace();
        }finally{  
            if(connector != null)
            {
                connector.shutdown();  
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
    public String getGroupId() {
        return groupId;
    }
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }
    
    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    public MessageExecutor getExecutor() {
        return executor;
    }
    public void setExecutor(MessageExecutor executor) {
        this.executor = executor;
    }
	public int getPartitionsNum() {
		return partitionsNum;
	}
	public void setPartitionsNum(int partitionsNum) {
		this.partitionsNum = partitionsNum;
	}

}


