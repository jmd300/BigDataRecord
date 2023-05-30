package com.zoo.lang.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @Author: JMD
 * @Date: 5/30/2023

 * 基于接口的JDK动态代理：该方式只能代理实现了某个接口的目标对象，它利用Java反射机制在运行时动态地创建一个实现目标对象接口的代理对象。
 * 使用InvocationHandler接口实现了一个代理对象，并在目标对象的方法执行前后添加了一些逻辑操作。
 */
public class UserDaoJdkProxy implements InvocationHandler {
    private final Object target;
    public UserDaoJdkProxy(Object target) {
        this.target = target;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // 在目标对象的方法执行前添加逻辑操作
        System.out.println("before " + method.getName());

        // 执行目标对象的方法
        Object result = method.invoke(target, args);

        // 在目标对象的方法执行后添加逻辑操作
        System.out.println("after " + method.getName());
        return result;
    }
    public static void main(String[] args) {
        IUserDao userDao = new UserDao();
        UserDaoJdkProxy proxy = new UserDaoJdkProxy(userDao);

        IUserDao userDaoProxy = (IUserDao) Proxy.newProxyInstance(
                userDao.getClass().getClassLoader(),
                userDao.getClass().getInterfaces(),
                proxy);
        userDaoProxy.addUser();
        userDaoProxy.deleteUser();
    }
}
