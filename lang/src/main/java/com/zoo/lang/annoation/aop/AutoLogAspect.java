package com.zoo.lang.annoation.aop;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

@Aspect
public class AutoLogAspect {
    /**
     * 只要用到了com.javaxl.p2.annotation.springAop.MyLog这个注解的，就是目标类
     */
    @Pointcut("@annotation(com.zoo.lang.annoation.aop.AutoLog)")
    //    这是以前的写法 @Around("execution"(* *..*Service.*Pager(..))")
    //   上面这个已经把这个替代掉了 @Pointcut("@execution(* *.*Controller.add())")
    private void MyValid() {
        System.out.println("MyValid");
    }

    @Before("MyValid()")
    public void before(JoinPoint joinPoint) {
        //joinPoint
        //目标对象、目标方法、传递的参数
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        System.out.println("[" + signature.getName() + " : start.....]");

        AutoLog myLog = signature.getMethod().getAnnotation(AutoLog.class);
        System.out.println("【目标对象方法被调用时候产生的日志，记录到日志表中】：" + myLog.desc());
    }

}
