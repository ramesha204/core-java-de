package org.example.spark;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class KafkaConsumerDemoT1 {
    public static void main(String[] args) {
        String winUtilPath = "C:\\softwares\\winutils\\";
        if(System.getProperty("os.name").toLowerCase().contains("win")) {
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

        StructType newsSchema = new StructType()
                .add("ticker", DataTypes.StringType)
                .add("date", DataTypes.DateType)
                .add("news", DataTypes.StringType);
        Dataset<Row> df = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("startingOffsets", "earliest")
                .option("subscribe", "news")
                .load()
                .select(from_json(col("value").cast("string"),newsSchema).alias("news_json"));


        StructType priceSchema = new StructType()
                .add("ticker", DataTypes.StringType)
                .add("date", DataTypes.StringType)
                .add("price", DataTypes.DoubleType);
       Dataset<Row> parsedDf =  df.selectExpr("timestamp", "offset", "news_json.*");
        StreamingQuery query;
        try {
//            query = df.withColumn("value_string",
//                            df.col("value").cast("string"))
//                    .writeStream().outputMode("append")
//                    .format("console").start();

            query  = parsedDf
                    .writeStream().outputMode("append")
                    .format("console").start();
            query.awaitTermination();
        } catch (TimeoutException | StreamingQueryException e) {
            e.printStackTrace();
        }


    }
}
