package org.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountSimple {
    public static void main(String[] args) {
        String url = "file:///C:\\Training\\sockgen_spark_java\\hello_world.txt";

        SparkSession spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> lines = sparkContext.textFile(url);
        lines.take(5).forEach(System.out::println);
      JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
               .filter(word -> !word.equals("is"));
      //words.take(5).forEach(System.out::println);
//        //remove any word that is an article a, an, to, in, the, as, is
       JavaPairRDD<String,Integer> pairRDD = words.mapToPair(w -> new Tuple2<>(w, 1));
       pairRDD.take(10).forEach(System.out::println);
       JavaPairRDD<String, Integer> counts = pairRDD.reduceByKey((sum, current) ->  sum + current);

      counts.collect().forEach(System.out::println);
//        //counts.saveAsTextFile("path to file");
//
//        try {
//            Thread.sleep(100000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

    }
}
