import java.io.IOException;

/**
 * @Author: JMD
 * @Date: 6/21/2023
 */
public class RecursionAlgorithmMain {
    public static int sigma(int n) {
        System.out.println("current 'n' value is " + n);
        return n + sigma(n + 1);
    }

    public static void main(String[] args) throws IOException {
        new Thread(() -> sigma(1)).start();
        System.in.read();
    }
}
