package sort.java;

import java.util.*;

/**
 * @Author: JMD
 * @Date: 5/6/2023
 */
public class BubbleSort<T> {
    static void _sort(int[] data){
        _sort(data, false);
    }
    /**
     * 不使用泛型的冒泡排序，原地排序
     * @param data 输入的整型数组
     */
    static void  _sort(int[] data, boolean isDesc){
        for(int i = 0; i < data.length - 1; i++){
            for(int j = 0; j < data.length - i - 1; j++){
                if(data[j] > data[j + 1] ^ isDesc){
                    int temp = data[j + 1];
                    data[j + 1] = data[j];
                    data[j] = temp;
                }
            }
        }
    }
    void sort(List<T> data, Comparator<? super T> c){
        for(int i = 0; i < data.size() - 1; i++){
            for(int j = 0; j < data.size() - i - 1; j++){
                if(c.compare(data.get(j), data.get(j + 1)) > 0){
                    T temp = data.get(j + 1);
                    data.set(j + 1, data.get(j));
                    data.set(j, temp);
                }
            }
        }
    }

    public static void main(String[] args) {
        int[] arr1 = {1, 5, 8, 7, 3, 4, 2, 9, 1};
        List<Integer> arr2= new ArrayList<>();
        for (int i : arr1) {
            arr2.add(i);
        }

        List<String> arr3 = new ArrayList<>();
        arr3.add("java");
        arr3.add("scala");
        arr3.add("scala");
        arr3.add("hadoop");
        arr3.add("spark");

        _sort(arr1);
        System.out.println(Arrays.toString(arr1));
        _sort(arr1, true);
        System.out.println(Arrays.toString(arr1));

        BubbleSort<Integer> objectBubbleSort = new BubbleSort<>();
        objectBubbleSort.sort(arr2, (i1, i2) -> i1 - i2);
        System.out.println(arr2);
        objectBubbleSort.sort(arr2, (i1, i2) -> -(i1 - i2));
        System.out.println(arr2);
        new BubbleSort<String>().sort(arr3, String::compareTo);
        System.out.println(arr3);
    }
}
