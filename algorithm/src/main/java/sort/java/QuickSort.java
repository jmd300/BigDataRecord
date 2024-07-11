package sort.java;

import java.util.Arrays;

/**
 * @Author: JMD
 * @Date: 5/6/2023
 * 学习 chatgbt 写的快速排序的程序，太强了！
 */
public class QuickSort {
    public static void sort(int[] arr, int left, int right) {
        if (left >= right) return;

        int pivot = partition(arr, left, right);
        sort(arr, left, pivot - 1);
        sort(arr, pivot + 1, right);
    }

    private static int partition(int[] arr, int left, int right) {
        int pivot = arr[left];
        int i = left + 1;
        int j = right;
        while (i <= j) {
            if (arr[i] > pivot && arr[j] < pivot) swap(arr, i++, j--);
            // arr[i]是从左到右第一个大于pivot的数据
            if (arr[i] <= pivot) i++;
            // arr[j]是从右到左第一个小于pivot的数据
            if (arr[j] >= pivot) j--;
        }
        swap(arr, left, j);
        return j;
    }

    public static void swap(int[] arr, int i, int j) {
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    public static void main(String[] args) {
        int[] arr = {4, 2, 1, 6, 7};
        QuickSort.sort(arr, 0, arr.length - 1);
        System.out.println(Arrays.toString(arr));
    }
}
