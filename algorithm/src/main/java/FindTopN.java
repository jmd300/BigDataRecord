import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * @Author: JMD
 * @Date: 5/9/2023
 */
public class FindTopN {
    public static int[] findBottomOrTopN(int[] nums, int n, boolean isTopN){
        PriorityQueue<Integer> pq;
        if(isTopN){
            pq = new PriorityQueue<>(n);
        }else{
            pq = new PriorityQueue<>(n, (i1, i2) -> -(i1 - i2));
        }
        for (int num : nums) {
            if (pq.size() < n) {
                pq.offer(num);
                //如果堆顶元素 > 新数，则删除堆顶，加入新数入堆
            } else if ((pq.peek() < num && isTopN) || (pq.peek() > num && !isTopN)) {
                pq.poll();
                pq.offer(num);
            }
        }
        // 从小/大顶堆中取出最终结果
        int[] result = new int[n];
        for (int i = 0; i < n && !pq.isEmpty(); i++) {
            result[i] = pq.poll();
        }
        return result;
    }

    public static int[] findBottomN(int[] nums, int n){
        return findBottomOrTopN(nums, n, false);
    }
    // 找出前N个最大数，采用小顶堆实现
    // PriorityQueue默认是自然顺序排序，要选择最大的k个数，构造小顶堆，每次取数组中剩余数与堆顶的元素进行比较，如果新数比堆顶元素大，则删除堆顶元素，并添加这个新数到堆中。
    public static int[] findTopN(int[] nums, int n) {
        return findBottomOrTopN(nums, n, true);
    }

    public static void main(String[] args) {
        Random random = new Random(0);
        int[]arr = new int[20];

        for (int i = 0; i < arr.length; i++) {
            arr[i] = random.nextInt();
        }
        System.out.println(Arrays.toString(arr));

        System.out.println(Arrays.toString(findTopN( arr,5)));

        System.out.println(Arrays.toString(findBottomN( arr,5)));
    }
}
