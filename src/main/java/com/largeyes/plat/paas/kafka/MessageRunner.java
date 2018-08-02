package com.largeyes.plat.paas.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class MessageRunner implements Runnable{  
    private KafkaStream<String, Message> partition;  
    private MessageExecutor executor;
    
    MessageRunner(KafkaStream<String, Message> partition,   MessageExecutor executor) {  
        this.partition = partition;
        this.executor = executor;
    }  
      
    public void run(){  
        ConsumerIterator<String, Message> it = partition.iterator();  
        while(it.hasNext()){  
            //connector.commitOffsets();手动提交offset,当autocommit.enable=false时使用  
            MessageAndMetadata<String, Message> item = it.next(); 
            try {
                executor.execute(item.message());//UTF-8,注意异常  
            } catch (Exception e) {
                e.printStackTrace();
            }
        }  
    }  
}

