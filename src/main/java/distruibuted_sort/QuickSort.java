package distruibuted_sort;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

//快速排序
public class QuickSort {

    public static JavaRDD<Integer> quickSort(JavaRDD<Integer> data) {
        return data.mapPartitions(iter -> {
            List<Integer> list = new ArrayList<>();
            iter.forEachRemaining(list::add);
            List<Integer> sortedList = quickSort(list,0,list.size());
            return sortedList.iterator();
        }, true);
    }

    public static List<Integer> quickSort(List<Integer> list, int low, int high) {
        if (low < high) {
            int pivotIndex = partition(list, low, high);
            quickSort(list, low, pivotIndex - 1);
            quickSort(list, pivotIndex + 1, high);
        }
        return list;
    }

    private static int partition(List<Integer> list, int low, int high) {
        int pivot = list.get(high);
        int i = low - 1;

        for (int j = low; j < high; j++) {
            if (list.get(j) < pivot) {
                i++;
                int temp = list.get(i);
                list.set(i, list.get(j));
                list.set(j, temp);
            }
        }

        int temp = list.get(i + 1);
        list.set(i + 1, list.get(high));
        list.set(high, temp);

        return i + 1;
    }
}