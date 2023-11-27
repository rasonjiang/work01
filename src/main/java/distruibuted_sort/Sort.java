package distruibuted_sort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Sort {
    public static void main(String[] args) {
        // 创建Spark配置和SparkContext
        SparkConf conf = new SparkConf().setAppName("DistributedSort");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 生成随机数据
        int dataSize = 1000000;
        JavaRDD<Integer> data = (JavaRDD<Integer>) sc.parallelize(createData(dataSize));

        // 进行排序
        JavaRDD<Integer> sortedData = data.sortBy(x -> x, true, dataSize);

        // 将结果收集到本地
        System.out.println(sortedData.take(10));  // 打印前10个元素

        // 关闭SparkContext
        sc.stop();
    }

    private static Iterable<Integer> createData(int dataSize) {
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