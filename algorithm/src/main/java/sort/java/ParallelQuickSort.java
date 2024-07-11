package sort.java;

import com.zoo.MyUtils;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;


@Data
@AllArgsConstructor
public class ParallelQuickSort extends RecursiveAction {
    private int [] array;
    private int left;
    private int right;

    private int partition(int left, int right){
        // 随便获取一个数作为分解值
        int pivot = array[left];

        // 左边第一个不再需要比较判断
        int i = left + 1;

        int j = right;

        while (i <= j && j >= left && i <= right){
            if (array[i] > pivot && array[j] < pivot) MyUtils.swapIntArray(array, i++, j--);
            if (array[i] <= pivot) i++;
            if (array[j] >= pivot) j--;
        }

        MyUtils.swapIntArray(array, left, j);
        return j;
    }

    @Override
    protected void compute(){
        if(left < right) {
            int partitionIndex = partition(left, right);

            // Parallelize the two subtasks
            ParallelQuickSort leftTask = new ParallelQuickSort(array, left, partitionIndex - 1);
            ParallelQuickSort rightTask = new ParallelQuickSort(array, partitionIndex + 1, right);

            invokeAll(leftTask, rightTask);

/*            leftTask.fork();
            rightTask.fork();

            leftTask.join();
            rightTask.join();*/
        }
    }

    public static void parallelQuickSort(int[] array){
        ForkJoinPool pool = new ForkJoinPool();
        pool.invoke(new ParallelQuickSort(array, 0, array.length - 1));
    }

    public static void main(String[] args) {
        int[] array = {12, 35, 87, 26, 9, 28, 7};
        parallelQuickSort(array);
        for(int i: array){
            System.out.print(i + " ");
        }
    }

}
