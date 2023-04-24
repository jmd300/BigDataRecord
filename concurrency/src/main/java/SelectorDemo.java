import java.io.IOException;
import java.nio.channels.Selector;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

/**
 * @Author: JMD
 * @Date: 4/20/2023
 */
public class SelectorDemo {
    public static void main(String[] args) throws IOException {
        Selector selector = Selector.open();
        selector.select();

        ReentrantReadWriteLock lock = null;
        StampedLock stampLock = null;
    }
}
