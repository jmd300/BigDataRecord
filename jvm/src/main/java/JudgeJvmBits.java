/**
 * @Author: JMD
 * @Date: 4/26/2023
 */
public class JudgeJvmBits {
    public static void main(String[] args) {
        String arch = System.getProperty("os.arch");
        System.out.println("JVM architecture is: " + arch);

        if (arch.contains("64")) {
            System.out.println("JVM is running in 64-bit mode.");
        } else {
            System.out.println("JVM is running in 32-bit mode.");
        }
    }
}
