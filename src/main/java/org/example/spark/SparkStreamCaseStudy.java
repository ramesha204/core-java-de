package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class SparkStreamCaseStudy {
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
                .appName("SocgenJavaStructuredStreamingCaseStudy")
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", "2");
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> maxTemperatures = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("file:///C:\\Training\\case_study\\max_temperature.csv");

        StructType deviceSchema = new StructType()
                .add("device_id", DataTypes.IntegerType)
                .add("temperature", DataTypes.DoubleType)
                .add("timestamp", DataTypes.TimestampType);
        Dataset<Row> deviceTemp = spark.readStream()
                .schema(deviceSchema)
                .option("header", "true")
                .csv("file:///C:\\Training\\case_study\\\\device_files");
        StreamingQuery query = null;
        try {
            Dataset<Row> joinedDf = deviceTemp.join(maxTemperatures,
                    deviceTemp.col("device_id").equalTo(maxTemperatures.col("device_id")),
                    "left_outer");
            query = joinedDf
                    .writeStream()
                    .outputMode("append")
                    .format("console")
                    .start();
            query.awaitTermination(100000);
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
