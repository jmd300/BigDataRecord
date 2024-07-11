package sort.java;

import java.util.Arrays;
import java.util.Queue;

/**
 * @Author: JMD
 * @Date: 5/6/2023
 *
 *
 */
public class MergeSort {
    static void merge(int [] arr, int left, int mid, int right){
        // 将这两个有序数组，合并成一个有序数组
        int s1 = left;
        int s2 = mid + 1;

        int i = 0;
        int [] ret = new int[right - left + 1];
        while (s1 <= mid && s2 <= right){
            if(arr[s1] <= arr[s2]){
                ret[i++] = arr[s1++];
            }else{
                ret[i++] = arr[s2++];
            }

            while (s1 <= mid){
                ret[i++] = arr[s1++];
            }
            while (s2 <= right){
                ret[i++] = arr[s2++];
            }
        }
        System.arraycopy(ret, 0, arr, left, ret.length);
    }

    static void sort(int [] arr, int left, int right){
        if(left >= right){
            return;
        }

        int mid = (left + right) >> 1;
        System.out.println("mid is: " + mid);
        // 1. 向下递归，划分任务
        sort(arr, left, mid);
        sort(arr, mid + 1, right);

        // 2. 将已经排好序的数组归并
        merge(arr, left, mid, right);

    }
    public static void main(String[] args) {
        int[] arr = {4, 2, 1, 6, 7};
        MergeSort.sort(arr, 0, arr.length - 1);
        System.out.println(Arrays.toString(arr));
    }
}
