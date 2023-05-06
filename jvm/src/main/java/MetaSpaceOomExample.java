/**
 * @Author: JMD
 * @Date: 4/26/2023
 */
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

public class MetaSpaceOomExample {
    static class OOM{
        // 这个静态变量是否final对类元信息的大小有影响
        final static String className = "OOM";
    }

    /**
     * 添加vm参数：-XX:MaxMetaspaceSize=10m
     * MetaSpace因为类元信息数量过多而内存溢出示例
     */
    public static void main(String[] args) {
        int i = 0;                              // 模拟计数多少次以后发生异常
        try {
            while (true){
                if(i / 100 == 0){
                    System.out.println("current: " + i);
                }
                i++;
                Enhancer enhancer = new Enhancer();
                enhancer.setSuperclass(OOM.class);
                enhancer.setUseCache(false);
                enhancer.setCallback(new MethodInterceptor() {
                    @Override
                    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
                        return methodProxy.invokeSuper(o,args);
                    }
                });
                enhancer.create();
            }
        } catch (Throwable e) {
            System.out.println("=================多少次后发生异常："+i);
            e.printStackTrace();
        }
    }
}

