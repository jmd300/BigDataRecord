package book;

public class Synchronized {
    public static void main(String[] args) {
        synchronized (Synchronized.class){
            System.out.println("同步代码块");
        }
    }
    public static synchronized void method(){
        System.out.println("同步方法");
    }
}
