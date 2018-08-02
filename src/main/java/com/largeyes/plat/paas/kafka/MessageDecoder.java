package com.largeyes.plat.paas.kafka;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

/**
 * 
 * Title: ECP <br>
 * Project Name:PlatPaas <br>
 * Description: <br>
 * Date:2016-1-6下午9:20:46  <br>
 * Copyright (c) 2016 asia All Rights Reserved <br>
 * 
 * @author PJieWin
 * @version  
 * @since JDK 1.6
 * 
 * 消息解码
 */
public class MessageDecoder implements Decoder<Message> {
    
    public MessageDecoder(VerifiableProperties props) {
        
    }
    
    public Message fromBytes(byte[] paramArrayOfByte) {
        try {
            ObjectInputStream  ois = new ObjectInputStream(new ByteArrayInputStream(paramArrayOfByte));
            return (Message)ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        
    }
    
    public static void main(String[] args) {
        Message txMsg = new Message();
        txMsg.setId(1);
        txMsg.setTopic("my-replicated-topic");
        txMsg.setMsg("msg|a"+System.currentTimeMillis());
        System.out.println(new String((new MessageEncoder(null)).toBytes(txMsg)));
        System.out.println(((new MessageDecoder(null)).fromBytes((new MessageEncoder(null)).toBytes(txMsg))).toString());
    }
}

