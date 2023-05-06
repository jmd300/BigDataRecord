/**
 * @Author: JMD
 * @Date: 5/6/2023
 */
public class PrintVM {
    static void print(){
        // 获取当前 Java 虚拟机
        Runtime runtime = Runtime.getRuntime();

        // 获取 Java 虚拟机的最大内存限制（-Xmx）
        long maxMemory = runtime.maxMemory();
        System.out.println("Maximum memory (Xmx): " + (maxMemory == Long.MAX_VALUE ? "no limit" : (maxMemory / 1024 / 1024 + " MB")));

        // 获取 Java 虚拟机的初始内存限制（-Xms）
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Initial memory (Xms): " + (initialMemory == Long.MAX_VALUE ? "no limit" : (initialMemory / 1024 / 1024 + " MB")));
    }

    public static void main(String[] args) {
        print();
    }
}
