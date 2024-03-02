package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.from_json;
import java.util.concurrent.TimeoutException;
public class KafkaAssignmentCode {
    public static Dataset<Row> readKafkaSource(SparkSession spark, String topicName,
                                               StructType schema) {
        Dataset<Row> dfRaw = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", topicName)
                .option("startingOffsets", "earliest")
                .load();
        Dataset<Row> dfParsed = dfRaw.withColumn("value_string",
                dfRaw.col("value").cast("string"));
        Dataset<Row> df = dfParsed.withColumn("json_value",
                        from_json(dfParsed.col("value_string"), schema))
                .selectExpr("timestamp", "offset", "json_value.*");
        return df;
    }
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
        StructType priceSchema = new StructType()
                .add("ticker", DataTypes.StringType)
                .add("date", DataTypes.DateType)
                .add("price", DataTypes.DoubleType);
        Dataset<Row> dfNews = readKafkaSource(spark, "news", newsSchema);
        Dataset<Row> dfPrice = readKafkaSource(spark, "price", priceSchema);

        Dataset<Row> priceStreamWithWatermark = dfPrice.
                withWatermark("timestamp", "1 hour");
        Dataset<Row>  newsStreamWithWaterMark = dfNews.
                withWatermark("timestamp", "1 minutes");
        StreamingQuery priceQuery;
        StreamingQuery newsQuery;
        StreamingQuery query;
        try {
//            priceQuery = dfPrice
//                    .writeStream().outputMode("append")
//                    .format("console").start();
//            newsQuery = dfNews
//                    .writeStream().outputMode("append")
//                    .format("console").start();
//            priceQuery.awaitTermination(10000000);
//            newsQuery.awaitTermination(10000000);

            Dataset<Row> joinedStreamWithWaterMark = priceStreamWithWatermark
                    .join(newsStreamWithWaterMark,
                    priceStreamWithWatermark.col("ticker")
                            .equalTo(newsStreamWithWaterMark.col("ticker"))
                            .and(priceStreamWithWatermark.col("timestamp")
                                    .geq(newsStreamWithWaterMark.col("timestamp")))

                    );
//            query = joinedStreamWithWaterMark
//                    .writeStream().outputMode("append")
//                    .format("console").start();
            query = joinedStreamWithWaterMark
                    .selectExpr( "to_json(struct(*)) AS value")
                    .writeStream().outputMode("append")
                    .format("kafka")
                    .outputMode("append")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", "output")
                    .option("checkpointLocation", "file:///C:/tmp/spark_checkpoint")
                    .start();
            query.awaitTermination(10000000);
        } catch (TimeoutException | StreamingQueryException e) {
            e.printStackTrace();
        }


    }
}
