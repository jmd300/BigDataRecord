package com.zoo.lang.annoation.aop;

import org.junit.Test;

public class AopTest {
    @AutoLog(desc = "测试aop日志")
    public void aopFunc(){
        System.out.println("启动测试");
    }

    @Test
    public void testAop(){
        this.aopFunc();
    }
}
