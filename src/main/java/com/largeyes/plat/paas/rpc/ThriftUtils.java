package com.largeyes.plat.paas.rpc;  
  
import java.util.Set;

import org.apache.thrift.TProcessor;
import org.springframework.util.ClassUtils;

public class ThriftUtils {

    public static Class<?> getIfaceClass(Class<?> implClass) {
        @SuppressWarnings("rawtypes")
        Set<Class> clazzArr = ClassUtils.getAllInterfacesForClassAsSet(implClass);
        Class<?> ifaceClass = null;
        for (Class<?> clazz : clazzArr) {
            String simpleName = clazz.getSimpleName();
            if (simpleName.equals("Iface")) {
                Class<?> declaringClass = clazz.getDeclaringClass();
                if (declaringClass != null) {
                    ifaceClass = clazz;
                    break;
                }
            }
        }
        return ifaceClass;
    }


    public static Class<?> getServiceClass(Class<?> ifaceInterface) {
        return ifaceInterface.getDeclaringClass();
    }


    @SuppressWarnings("unchecked")
    public static Class<TProcessor> getProcessorClass(Class<?> serviceClazz) {
        Class<?>[] declaredClasses = serviceClazz.getDeclaredClasses();
        for (Class<?> declaredClass : declaredClasses) {
            Class<?>[] interfaceClasses = declaredClass.getInterfaces();
            for (Class<?> interfaceClass : interfaceClasses) {
                if (interfaceClass == TProcessor.class) {
                    return (Class<TProcessor>) declaredClass;
                }
            }
        }
        return null;
    }
    
    
    public static String getServiceName(Class<?> ifaceClass) {
        Class<?> serviceClass = ThriftUtils.getServiceClass(ifaceClass);
        String serviceName = serviceClass.getSimpleName().toLowerCase().replace("service", "");
        return serviceName;
    }
}
