import java.util.concurrent.TimeUnit;

/**
 * @Author: JMD
 * @Date: 4/12/2023
 */
public class InterruptDemo {

    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()){
                System.out.println("Thread.currentThread().isInterrupted(): " + Thread.currentThread().isInterrupted());
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                    System.out.println("t1 " + Thread.currentThread().isInterrupted());
                    e.printStackTrace();
                    // 再次中断
                    Thread.currentThread().interrupt();
                    System.out.println("t1 " + Thread.currentThread().isInterrupted());
                }
            }
        });

        Thread t2 = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()){
                System.out.println("Thread.interrupted(): " + Thread.interrupted());
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                    System.out.println("t2 " + Thread.currentThread().isInterrupted());
                    e.printStackTrace();
                    // 再次中断
                    Thread.currentThread().interrupt();
                    System.out.println("t2 " + Thread.currentThread().isInterrupted());
                }
            }
        });

        t1.start();
        t2.start();

        TimeUnit.SECONDS.sleep(5);
        t1.interrupt();
        t2.interrupt();

        TimeUnit.SECONDS.sleep(100);
    }
}
