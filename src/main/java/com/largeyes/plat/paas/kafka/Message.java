package com.largeyes.plat.paas.kafka;

import java.io.Serializable;

public class Message implements Serializable{
    
    private static final long serialVersionUID = 6600083057297345019L;
    private int id;
    private String topic;
    private Object msg;
    
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    public Object getMsg() {
        return msg;
    }
    public void setMsg(Object msg) {
        this.msg = msg;
    }
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Message [id=");
        builder.append(id);
        builder.append(", topic=");
        builder.append(topic);
        builder.append(", msg=");
        builder.append(msg);
        builder.append("]");
        return builder.toString();
    }
}

