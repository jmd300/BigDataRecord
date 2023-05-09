package sort.java;

import java.util.Arrays;
import java.util.Random;

/**
 * @Author: JMD
 * @Date: 5/6/2023
 * 参考：https://blog.csdn.net/allway2/article/details/114003894
 *      https://blog.csdn.net/weixin_44491927/article/details/105120985
 * 计数排序是一个非基于比较的排序算法，该算法于1954年由 Harold H. Seward 提出。它的优势在于在对一定范围内的整数排序时，它的复杂度为Ο(n+k)（其中k是整数的范围），快于任何比较排序算法。
 */
public class CountingSort {
    public static void countingSort(int[] arr) {
        if (arr == null || arr.length < 2) {
            return;
        }
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        for (int k : arr) {
            max = Math.max(max, k);
            min = Math.min(min, k);
        }
        System.out.println("min is: " + min);
        // 辅助数组的含义：在arr中 等于 index 的数的数量为 count[index]
        int[] count = new int[max + 1 - min];
        for (int j : arr) {
            count[j - min]++;
        }

        System.out.println("辅助数组的含义：在arr中小于等于 index 的数的数量为 count[index]");
        for (int i = 1; i < count.length; i++) {
            count[i] += count[i - 1];
        }

        int[] tmp = new int[arr.length];

        for (int j : arr) {
            // 对于重复的元素从大到小放置
            int index = --count[j - min];
            tmp[index] = j;
        }

        // 可以在这里决定要正序排列还是逆序排列
        System.arraycopy(tmp, 0, arr, 0, arr.length);
    }

    public static void main(String[] args) {
        Random random = new Random(0);
        int[]arr = new int[20];

        for (int i = 0; i < arr.length; i++) {
            arr[i] = random.nextInt(200) - 100;
        }
        System.out.println(Arrays.toString(arr));


        countingSort(arr);
        System.out.println(Arrays.toString(arr));
    }
}
