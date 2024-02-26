package org.example.spark;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;


public class SparkStructuredStreamWordCount {

    public static void main(String[] args) {
        String winutilPath = "C:\\softwares\\winutils\\"; //\\bin\\winutils.exe"; //bin\\winutils.exe";

        if(System.getProperty("os.name").toLowerCase().contains("win")) {
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


        Dataset<Row> streamingFiles = spark.readStream().text("file:///C:\\Training\\sockgen_spark_java\\text_files");
        StreamingQuery query = null;
        try {
            Dataset<Row> words = streamingFiles.select(functions.explode(
                            functions.split(streamingFiles.col("value"), " "))
                    .alias("word"));

            query = words
                    .writeStream()
                    .outputMode("append")
                    .format("console")
                    .start();

        Dataset<Row> wordCounts = words.groupBy("word").count();
        StreamingQuery query2 = wordCounts.writeStream().outputMode("complete")
                .format("console").start();
            query.awaitTermination(100000);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
//        Dataset<Row> words = streamingFiles.select(functions.explode(
//                functions.split(streamingFiles.col("value"), " "))
//                .alias("word"));
//        Dataset<Row> wordCounts = words.groupBy("word").count();
//        StreamingQuery query = wordCounts.writeStream().outputMode("complete")
//                .format("console").start();
    }

}

