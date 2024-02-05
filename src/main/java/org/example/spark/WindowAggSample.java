package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class WindowAggSample {
    static String sales2Url = "file:///C:\\Training\\sockgen_spark_java\\sales_2.csv";
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> sales2Df = readSales(spark,sales2Url);
//       sales2Df.show();
        sales2Df.createOrReplaceTempView("sales");
        Dataset<Row> groupByDf = spark.sql("select date_of_sale,sum(total_amount) as item_total" +
                " from sales group by date_of_sale");
//        groupByDf.show();
        groupByDf.createOrReplaceTempView("item_total");
       Dataset<Row> groupJoinedDf = spark.sql("select s.*,i.item_total from " +
               "sales s join item_total i on s.date_of_sale=i.date_of_sale ");
//        groupJoinedDf.show();

        Dataset<Row> percentageSaleDf = spark.sql("select s.item_id,s.total_amount,s.date_of_sale, " +
                " (s.total_amount*100/i.item_total) as sale_item_percentage from " +
                "sales s join item_total i on s.date_of_sale=i.date_of_sale ");
        percentageSaleDf.show();


        WindowSpec  windowSpec = Window.partitionBy("date_of_sale");
        Dataset<Row> windowDf = sales2Df.withColumn("item_total",
                functions.sum("total_amount").over(windowSpec));
        windowDf.show();

    }

    public static Dataset<Row> readSales(SparkSession spark, String filePath){
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("item_id",  DataTypes.IntegerType, true),
                DataTypes.createStructField("item_qty", DataTypes.IntegerType, true),
                DataTypes.createStructField("unit_price", DataTypes.FloatType, true),
                DataTypes.createStructField("total_amount", DataTypes.IntegerType, true),
                DataTypes.createStructField("date_of_sale", DataTypes.DateType, true)
        });
        Dataset<Row> productDf = spark.read()
                .schema(schema)
                .option("header", "true")
                .csv(filePath);
        return productDf;
    }
}
