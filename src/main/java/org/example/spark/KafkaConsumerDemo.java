package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import java.util.concurrent.TimeoutException;
public class KafkaConsumerDemo {
    public static void main(String[] args) {
        String winUtilPath = "C:\\softwares\\winutils\\";
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            System.out.println("detected windows");
            System.setProperty("hadoop.home.dir", winUtilPath);
            System.setProperty("HADOOP_HOME", winUtilPath);
        }
        SparkSession spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", "2");
        spark.sparkContext().setLogLevel("WARN");
        Dataset<Row> df = spark.read().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("startingOffsets", "earliest")
                .option("subscribe", "socgenl2").load();

        df.withColumn("value_string",
                df.col("value").cast("string")).show();
//        StreamingQuery query;
//        try {
//            query = df.withColumn("value_string",
//                            df.col("value").cast("string"))
//                    .writeStream().outputMode("append")
//                    .format("console").start();
//            query.awaitTermination();
//        } catch (TimeoutException | StreamingQueryException e) {
//            e.printStackTrace();
//        }
//    }

    }
}

