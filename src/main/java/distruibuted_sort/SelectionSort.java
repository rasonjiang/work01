package distruibuted_sort;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

//归并排序
public class SelectionSort {

    public static JavaRDD<Integer> selectionSort(JavaRDD<Integer> data) {
        return data.mapPartitions(iter -> {
            List<Integer> list = new ArrayList<>();
            iter.forEachRemaining(list::add);
            List<Integer> sortedList = selectionSort(list);
            return sortedList.iterator();
        }, true);
    }

    public static List<Integer> selectionSort(List<Integer> list) {
        int n = list.size();

        for (int i = 0; i < n - 1; i++) {
            int minIndex = i;
            for (int j = i + 1; j < n; j++) {
                if (list.get(j) < list.get(minIndex)) {
                    minIndex = j;
                }
            }

            int temp = list.get(minIndex);
            list.set(minIndex, list.get(i));
            list.set(i, temp);
        }
        return list;
    }
}