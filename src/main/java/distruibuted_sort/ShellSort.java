package distruibuted_sort;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

//归并排序
public class ShellSort {

    public static JavaRDD<Integer> shellSort(JavaRDD<Integer> data) {
        return data.mapPartitions(iter -> {
            List<Integer> list = new ArrayList<>();
            iter.forEachRemaining(list::add);
            List<Integer> sortedList = shellSort(list);
            return sortedList.iterator();
        }, true);
    }

    public static List<Integer> shellSort(List<Integer> list) {
        int n = list.size();

        for (int gap = n / 2; gap > 0; gap /= 2) {
            for (int i = gap; i < n; i++) {
                int temp = list.get(i);
                int j;
                for (j = i; j >= gap && list.get(j - gap) > temp; j -= gap) {
                    list.set(j, list.get(j - gap));
                }
                list.set(j, temp);
            }
        }
        return list;
    }
}