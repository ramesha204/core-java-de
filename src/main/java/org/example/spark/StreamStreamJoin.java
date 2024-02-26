package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.current_timestamp;

public class StreamStreamJoin {
    public static void main(String[] args) {
        String winutilPath = "C:\\softwares\\winutils\\"; //\\bin\\winutils.exe"; //bin\\winutils.exe";
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            System.out.println("Detected windows");
            System.setProperty("hadoop.home.dir", winutilPath);
            System.setProperty("HADOOP_HOME", winutilPath);
        }
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SocgenJavaStructuredStreaming")
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", "2");
        spark.sparkContext().setLogLevel("WARN");
        Dataset<Row> wordTypes = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("file:///C:\\Training\\sockgen_spark_java\\word_types.csv");
        StructType newsSchema = new StructType()
                .add("ticker", DataTypes.StringType)
                .add("news_text", DataTypes.StringType)
                .add("news_time", DataTypes.TimestampType);

        Dataset<Row> newsStream = spark.readStream()
                .schema(newsSchema)
                .option("header", "true")
                .csv("file:///C:\\Training\\sockgen_spark_java\\news");
        Dataset<Row> newsStreamWithWaterMark = newsStream.
                withWatermark("news_time", "1 minutes");
        StructType priceSchema = new StructType()
                .add("ticker", DataTypes.StringType)
                .add("price", DataTypes.DoubleType)
                .add("price_time", DataTypes.TimestampType);
        Dataset<Row> priceStream = spark.readStream().schema(priceSchema)
                .option("header", "true")
                .csv("file:///C:\\Training\\sockgen_spark_java\\price");
        Dataset<Row> priceStreamWithWatermark = priceStream.
                withWatermark("price_time", "1 hour");

        StreamingQuery query = null;
        try {
            //join of the unbounded table
            Dataset<Row> joinedStream = priceStream.join(newsStream,
                    priceStream.col("ticker")
                            .equalTo(newsStream.col("ticker")));
            //join within the watermark window
//            Dataset<Row> joinedStreamWithWaterMark = priceStreamWithWatermark
//                    .join(newsStreamWithWaterMark,
//                    priceStreamWithWatermark.col("ticker")
//                            .equalTo(newsStreamWithWaterMark.col("ticker"))
//                            .and(priceStreamWithWatermark.col("arrivalTime")
//                                    .geq(newsStreamWithWaterMark.col("arrivalTime")))
//                            .and(priceStreamWithWatermark.col("arrivalTime")
//                                    .leq(newsStreamWithWaterMark.col("arrivalTime") + "1 hour"))
//                    );

//            joinedStream.withColumn("arrival_time",current_timestamp());
            query = joinedStream
                    .writeStream()
                    .outputMode("append")
                    .format("console")
                    .start();
            query.awaitTermination();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
