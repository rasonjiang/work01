package distruibuted_sort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DistributedSort {
    public static void main(String[] args) {
        // 创建SparkConf对象
        SparkConf conf = new SparkConf().setAppName("DistributedSort").setMaster("local");
        //创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

/*
        //读取数据（文本）
        JavaRDD<String> lines = sc.textFile("input.txt");
*/

        // 生成随机数据
        int dataSize = 1000;
        JavaRDD<Integer> data;
        data = (JavaRDD<Integer>) sc.parallelize(createData(dataSize));

        // 进行排序
        JavaRDD<Integer> sortedData ;
        //Spark API：sortBy排序
//        sortedData = data.sortBy(x -> x, true, dataSize);
        sortedData = MergeSort.mergeSort(data);

        // 将结果收集到本地
        System.out.println(sortedData.take(10));  // 打印前10个元素

        // 关闭SparkContext
        sc.close();
    }

    private static List<Integer> createData(int dataSize) {
        // 生成随机数据的逻辑
        // 这里可以根据实际需求生成你的随机数据
        // 这里简单地生成一组升序整数
        List<Integer> data = new ArrayList<>();
        for (int i = 1; i <= dataSize; i++) {
            data.add(i);
        }
        Collections.shuffle(data);  // 打乱顺序，模拟随机数据
        return data;
    }

}