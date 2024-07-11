package sort.java;

import java.util.concurrent.ForkJoinPool;
        import java.util.concurrent.RecursiveAction;
        import java.util.Arrays;

public class ForkJoinQuickSort extends RecursiveAction {

    private static final long serialVersionUID = 1L;
    private int[] array;
    private int low;
    private int high;

    public ForkJoinQuickSort(int[] array, int low, int high) {
        this.array = array;
        this.low = low;
        this.high = high;
    }

    @Override
    protected void compute() {
        if (high <= low) {
            return;
        }

        int pivotIndex = partition(array, low, high);

        ForkJoinQuickSort leftSort = new ForkJoinQuickSort(array, low, pivotIndex - 1);
        ForkJoinQuickSort rightSort = new ForkJoinQuickSort(array, pivotIndex + 1, high);

        invokeAll(leftSort, rightSort);
    }

    private int partition(int[] arr, int low, int high) {
        int pivot = arr[high];
        int i = (low - 1);
        for (int j = low; j < high; j++) {
            if (arr[j] <= pivot) {
                i++;

                int temp = arr[i];
                arr[i] = arr[j];
                arr[j] = temp;
            }
        }

        int temp = arr[i + 1];
        arr[i + 1] = arr[high];
        arr[high] = temp;

        return i + 1;
    }

    public static void main(String[] args) {
        int[] data = {10, 7, 8, 9, 1, 5};
        ForkJoinPool pool = new ForkJoinPool();
        ForkJoinQuickSort task = new ForkJoinQuickSort(data, 0, data.length - 1);
        pool.invoke(task);

        System.out.println(Arrays.toString(data));
    }
}
