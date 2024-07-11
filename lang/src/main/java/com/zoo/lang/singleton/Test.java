package com.zoo.lang.singleton;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class Test {
    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IOException, ClassNotFoundException {
        AudiCar car1 = AudiCar.INSTANCE;
        AudiCar car2 = AudiCar.INSTANCE;

        System.out.println("car1:");
        car1.run();
        System.out.println("car2:");
        car2.run();

        System.out.println("car1 == car2 ?");
        System.out.println(car1 == car2);

        // 能不能通过反射破坏单例模式？
        // Exception in thread "main" java.lang.NoSuchMethodException: com.zoo.lang.singleton.AudiCar.<init>()
        try {
            Constructor<AudiCar> constructor = AudiCar.class.getConstructor();
            AudiCar car3 = constructor.newInstance();
            System.out.println("car3");
            car3.run();
            System.out.println("car1 == car3 ?");
            System.out.println(car1 == car3);
        }
        catch (Exception e){
            e.printStackTrace();
        }

        try {
            // 这样也是会报异常的，没有这个构造函数
            // Exception in thread "main" java.lang.NoSuchMethodException: com.zoo.lang.singleton.AudiCar.<init>()
            Constructor<AudiCar> constructor1 =AudiCar.class.getDeclaredConstructor();
            constructor1.setAccessible(true);
            AudiCar car4 = constructor1.newInstance();
            System.out.println("car4");
            car4.run();
            System.out.println("car1 == car4 ?");
            System.out.println(car1 == car4);
        }catch (Exception e){
            e.printStackTrace();
        }

        // 能不能通过反序列化破坏单例模式？
        // 通过反序列化可以创造出对象，但是由于枚举类的初始化方式，这里反序列化的对象还是和原来的对象相等，是一个对象，没有破坏单例模式。
        // TODO: 枚举类单例对象的底层原理？

        //Write obj to file
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("tempFile"));
        oos.writeObject(car1);
        //Read Obi from file
        File file = new File("tempFile");
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
        AudiCar car5 = (AudiCar)ois.readObject();
        System.out.println("car5");
        car5.run();
        //判断是否是同一个对象
        System.out.println(car5 == car1);
    }
}
