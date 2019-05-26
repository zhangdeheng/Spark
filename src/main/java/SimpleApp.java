import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @Author: zhangdeheng
 * @Date: 2019-05-26 00:14
 * @Version 1.0
 */
public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "hdfs:///user/zhangdeheng/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs =logData.filter((FilterFunction<String>) s->
            s.contains("a")
        ).count();
        long numBs =logData.filter((FilterFunction<String>) s->
                s.contains("b")
        ).count();
        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
