package distruibuted_sort;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

//基数排序
public class RadixSort {

    public static JavaRDD<Integer> radixSort(JavaRDD<Integer> data) {
        return data.mapPartitions(iter -> {
            List<Integer> list = new ArrayList<>();
            iter.forEachRemaining(list::add);
            List<Integer> sortedList = radixSort(list);
            return sortedList.iterator();
        }, true);
    }

    public static List<Integer> radixSort(List<Integer> list) {
        int max = getMax(list);

        for (int exp = 1; max / exp > 0; exp *= 10) {
            countingSort(list, exp);
        }
        return list;
    }

    private static int getMax(List<Integer> list) {
        int max = list.get(0);
        for (int num : list) {
            if (num > max) {
                max = num;
            }
        }
        return max;
    }

    private static void countingSort(List<Integer> list, int exp) {
        int n = list.size();
        List<Integer> output = new LinkedList<>();
        int[] count = new int[10];

        for (int i = 0; i < 10; i++) {
            count[i] = 0;
        }

        for (int i = 0; i < n; i++) {
            count[(list.get(i) / exp) % 10]++;
        }

        for (int i = 1; i < 10; i++) {
            count[i] += count[i - 1];
        }

        for (int i = n - 1; i >= 0; i--) {
            output.add(0, 0);
        }
        for (int i = n - 1; i >= 0; i--) {
            output.set(count[(list.get(i) / exp) % 10] - 1, list.get(i));
            count[(list.get(i) / exp) % 10]--;
        }

        for (int i = 0; i < n; i++) {
            list.set(i, output.get(i));
        }
    }
}