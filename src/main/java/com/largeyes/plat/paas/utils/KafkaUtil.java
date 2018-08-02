package com.largeyes.plat.paas.utils;

import com.largeyes.plat.paas.kafka.MessageProducer;

public class KafkaUtil {
    private static MessageProducer producer;
    static{
        producer = PaasUtilsContextHolder.getBean("messageProducer", MessageProducer.class);
    }
    public static void sendMessage(Object message, String topic) {
        producer.sendMessage(message, topic);
    }
}

