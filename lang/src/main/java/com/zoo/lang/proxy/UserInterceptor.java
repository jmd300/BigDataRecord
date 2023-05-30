package com.zoo.lang.proxy;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

/**
 * @Author: JMD
 * @Date: 5/30/2023
 *
 *
 */
public class UserInterceptor implements MethodInterceptor {
    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        // 在目标对象的方法执行前添加逻辑操作
        System.out.println("before " + method.getName());
        // 执行目标对象的方法
        Object result = proxy.invokeSuper(obj, args);
        // 在目标对象的方法执行后添加逻辑操作
        System.out.println("after " + method.getName());
        return result;
    }

    public static void main(String[] args) {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(User.class);
        enhancer.setCallback(new UserInterceptor());
        User UserProxy = (User) enhancer.create();
        UserProxy.addUser();
        UserProxy.deleteUser();
    }
}
