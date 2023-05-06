import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.ArrayList;
import java.util.List;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

// 定义一个类
class Person implements Cloneable, Serializable {
    private String name;
    private int age;
    private List<String> hobbies;
    public Person(){

    }

    public Person(String name, int age, List<String> hobbies) {
        this.name = name;
        this.age = age;
        this.hobbies = hobbies;
    }

    // 重写Object类的clone()方法
    @Override
    public Object clone() throws CloneNotSupportedException {
        // 调用父类的clone()方法得到一个浅拷贝的对象
        Person clone = (Person) super.clone();
        // 对引用类型进行深拷贝
        clone.hobbies = this.hobbies == null ? null : new ArrayList<>(this.hobbies);
        return clone;
    }

    public void sayHello() {
        System.out.println("Hello, my name is " + name + ", and I'm " + age + " years old.");
        System.out.println("My hobbies are: " + hobbies);
    }
}

/**
 * @Author: JMD
 * @Date: 4/28/2023
 * Java中生成对象的5种方式
 * 1. new
 * 2. Class.newInstance
 * 3. Constructor
 * 4. clone
 * 5. 反序列化
 */
public class CreateObject {
    public static void main(String[] args) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, CloneNotSupportedException {
        Person person1 = new Person("xm", 20, null);
        Person person2 = person1.getClass().newInstance();
        person2.sayHello();

        Constructor<Person> constructor = Person.class.getConstructor(String.class, int.class, List.class);
        Person person3 = constructor.newInstance("xl", 18, null);
        person3.sayHello();

        Person person4 = (Person) person1.clone();
        person4.sayHello();

        // 序列化对象
        Path objectPath = Paths.get("person.dat");
        try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(objectPath))) {
            out.writeObject(person1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 反序列化对象
        try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(objectPath))) {
            Person person5 = (Person) in.readObject();
            System.out.println(person5);
            person5.sayHello();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
