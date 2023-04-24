import org.openjdk.jol.info.ClassLayout;

/**
 * @Author: JMD
 * @Date: 4/23/2023
 */
public class ObjectSIzeDemo {
    public static void main(String[] args) {
        ClassLayout classLayout = ClassLayout.parseInstance(new EmptyObject());
        System.out.println(classLayout.toPrintable());
    }
}
class EmptyObject{
    
}
