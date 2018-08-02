package com.largeyes.plat.paas.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;

import com.alibaba.fastjson.JSONObject;
import com.largeyes.plat.paas.rmc.ConfigurationCenter;
import com.largeyes.plat.paas.rmc.DefaultConfigurationWatcher;
import com.largeyes.plat.paas.utils.StringUtil;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
/**
 * 
 * Title: ECP <br>
 * Project Name:PlatPaas <br>
 * Description: <br>
 * Date:2016-1-6下午9:07:58  <br>
 * Copyright (c) 2016 asia All Rights Reserved <br>
 * 
 * @author PJieWin
 * @version  
 * @since JDK 1.6
 * 
 * Kafka消息发送对象
 */
public class MessageProducer extends DefaultConfigurationWatcher {
    public static final Logger log = Logger.getLogger(MessageProducer.class);

    private String configpath = "/config/plat/paas/kafka/producer";

    private ConfigurationCenter cc = null;

    private Producer<String, Message> producer = null;

    private Properties props = null;
    
    private Random random = new Random();

    
    public MessageProducer()
    {
        
    }
    public MessageProducer(ConfigurationCenter cc, String configpath)
    {
        this.cc = cc;
        this.configpath = configpath;
    }
    public void init()
    {
        if (log.isInfoEnabled()) {
            log.info("init MessageSender...");
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
            log.info("new MessageSender configuration is received: " + producerConfig);
        }
        JSONObject json = JSONObject.parseObject(producerConfig);
        @SuppressWarnings("rawtypes")
        Set<String> keys = json.keySet();
        boolean changed = false;
        if (keys != null) {
            for (String key:keys) {
                if (props == null) {
                    props = new Properties();
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
            ProducerConfig cfg = new ProducerConfig(props);
            producer = new Producer<String, Message>(cfg);
        }
    }
    
    public void sendMessage(Object message, String topic) {
    	
    	
        if(StringUtil.isBlank(topic) || StringUtil.isEmpty(message))
        {
            return ;
        }
        
        Message msg = new Message();
        msg.setId( Math.abs(random.nextInt()));
        msg.setTopic(topic);
        msg.setMsg(message);
        KeyedMessage<String, Message> km = new KeyedMessage<String, Message>(msg.getTopic(),String.valueOf(msg.getId()), msg);

        producer.send(km);
    }
    public void sendMessage(Collection<Object> messages, String topic) {
        if(StringUtil.isBlank(topic) || StringUtil.isEmpty(messages))
        {
            return ;
        }
        
        List<KeyedMessage<String, Message>> kms = new ArrayList<KeyedMessage<String,Message>>();
        for(Object me : messages)
        {
            Message msg = new Message();
            msg.setId( Math.abs(random.nextInt()));
            msg.setTopic(topic);
            msg.setMsg(me);
            KeyedMessage<String, Message> km = new KeyedMessage<String, Message>(msg.getTopic(),String.valueOf(msg.getId()), msg);
            
            kms.add(km);
        }
        producer.send(kms);
    }
    public String getConfigpath() {
        return configpath;
    }
    public void setConfigpath(String configpath) {
        this.configpath = configpath;
    }
    public ConfigurationCenter getCc() {
        return cc;
    }
    public void setCc(ConfigurationCenter cc) {
        this.cc = cc;
    }
    
    
}

