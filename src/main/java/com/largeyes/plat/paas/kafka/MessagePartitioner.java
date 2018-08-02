package com.largeyes.plat.paas.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 
 * Title: ECP <br>
 * Project Name:PlatPaas <br>
 * Description: <br>
 * Date:2016-1-6下午9:10:58  <br>
 * Copyright (c) 2016 asia All Rights Reserved <br>
 * 
 * @author PJieWin
 * @version  
 * @since JDK 1.6
 * 
 * 消息发送到指定分区规则
 */
public class MessagePartitioner implements Partitioner{

    public MessagePartitioner(VerifiableProperties props) {
        
    }
    
    public int partition(Object key, int partitionNumber) {
        int partition = 0;
        try {
            partition = Math.abs(Integer.parseInt(key.toString()) % partitionNumber);
            
        } catch (Exception e) {
            // TODO: handle exception
            partition = Math.abs(key.hashCode() % partitionNumber);
        }
        return partition;

    }
    
    
}

