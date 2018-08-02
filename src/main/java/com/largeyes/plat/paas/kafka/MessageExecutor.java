package com.largeyes.plat.paas.kafka;

public interface MessageExecutor {
    public abstract void execute(Message message) throws Exception;  
}

