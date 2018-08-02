package com.largeyes.plat.paas.utils;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


public class PaasUtilsContextHolder {
    private static ApplicationContext ctx;
    
    public static ApplicationContext getContext() {
    	if (ctx == null) {
    		synchronized (ApplicationContext.class){
    			if (ctx == null) {
            		ctx = new ClassPathXmlApplicationContext(new String[] { "PaasUtilsContext.xml" });
				}
    		}
		}
        return ctx;
    }
    public static void setContext(ApplicationContext context){
    	ctx = context;
    }
    public static <T> T getBean(String name, Class<T> clazz){
                
        return PaasUtilsContextHolder.getContext().getBean(name, clazz);
    }
}

