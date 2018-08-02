package com.largeyes.plat.paas.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtil {
    
    private static final String LOG_THREAD_HEADER = "[THREAD_ID:";
    private static final String LOG_THREAD_TAIL = "];";
    
    private static String buildMsg(String msg){
        return LOG_THREAD_HEADER + ThreadId.getThreadId() + LOG_THREAD_TAIL + msg;
    }
    
    public static void debug(String module,String msg){
        Logger logger = LoggerFactory.getLogger(module);
        if(logger.isDebugEnabled()){
            logger.debug(buildMsg(msg));
        }
    }
    
    public static void debug(String module,String msg,Throwable t){
        Logger logger = LoggerFactory.getLogger(module);
        if(logger.isDebugEnabled()){
            logger.debug(buildMsg(msg),t);
        }
    }
    
    public static void error(String module,String msg){
        Logger logger = LoggerFactory.getLogger(module);
        if(logger.isErrorEnabled()){
            logger.error(buildMsg(msg));
        }
    }
    
    public static void error(String module,String msg,Throwable t){
        Logger logger = LoggerFactory.getLogger(module);
        if(logger.isErrorEnabled()){
            logger.error(buildMsg(msg),t);
        }
    }
    
    public static void info(String module,String msg){
        Logger logger = LoggerFactory.getLogger(module);
        if(logger.isInfoEnabled()){
            logger.info(buildMsg(msg));
        }
    }
    
    public static void info(String module,String msg,Throwable t){
        Logger logger = LoggerFactory.getLogger(module);
        if(logger.isInfoEnabled()){
            logger.info(buildMsg(msg),t);
        }
    }

    public static void warn(String module,String msg){
        Logger logger = LoggerFactory.getLogger(module);
        if(logger.isWarnEnabled()){
            logger.warn(buildMsg(msg));
        }
    }
    
    public static void warn(String module,String msg,Throwable t){
        Logger logger = LoggerFactory.getLogger(module);
        if(logger.isWarnEnabled()){
            logger.warn(buildMsg(msg),t);
        }
    }
    

}

