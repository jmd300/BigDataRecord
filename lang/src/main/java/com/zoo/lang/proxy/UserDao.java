package com.zoo.lang.proxy;

/**
 * @Author: JMD
 * @Date: 5/30/2023
 */
public class UserDao implements IUserDao{
    @Override
    public void addUser() {
        System.out.println("添加用户成功");
    }

    @Override
    public void deleteUser() {
        System.out.println("删除用户成功");
    }
}
