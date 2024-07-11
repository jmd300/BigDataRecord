package com.zoo;

public class MyUtils {

    public static void swapIntArray(int[] arr, int left, int right){
        int temp = arr[right];
        arr[right] = arr[left];
        arr[left] = temp;
    }
}
