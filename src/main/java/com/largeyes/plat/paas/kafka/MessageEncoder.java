package com.largeyes.plat.paas.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * 
 * Title: ECP <br>
 * Project Name:PlatPaas <br>
 * Description: <br>
 * Date:2016-1-6下午9:20:58  <br>
 * Copyright (c) 2016 asia All Rights Reserved <br>
 * 
 * @author PJieWin
 * @version  
 * @since JDK 1.6
 * 
 * 消息编码
 */
public class MessageEncoder implements Encoder<Message> {
    
    public MessageEncoder(VerifiableProperties props) {
    	
    }

    public byte[] toBytes(Message msg) {
    	    	
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(msg);
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}

