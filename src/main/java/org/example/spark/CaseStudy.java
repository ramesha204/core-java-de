package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class CaseStudy {
    static String return_url = "file:///C:\\Training\\sockgen_spark_java\\dataset\\Global Superstore Sales - Global Superstore Returns.csv";
    static String sales_url = "file:///C:\\Training\\sockgen_spark_java\\dataset\\Global Superstore Sales - Global Superstore Sales.csv";
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> salesDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(sales_url);
        Dataset<Row> returnsDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(return_url);

        Dataset<Row> modifiedReturnsDf = returnsDf.withColumnRenamed("order id","order_id");

        Dataset<Row> modifiedSalesDf = salesDf.withColumnRenamed("order id","order_id")
                .withColumnRenamed("Sub-Category","sub_category")
                .withColumn("timestamp",to_date(col("order date"),"M/d/yyyy"))
                .withColumn("year",date_format(col("timestamp"),"yyyy"))
                .withColumn("month",date_format(col("timestamp"),"M"))
                .withColumn("profit_amount",regexp_replace(col("profit"),"[$,]","").cast(DataTypes.FloatType));

        Dataset<Row> joinedDf = modifiedSalesDf.join(modifiedReturnsDf,modifiedSalesDf.col("order_id")
                .equalTo(modifiedReturnsDf.col("order_id")),"left_anti");

        //solution1 : using groupBy on specified columns to get aggregated profit and quantity
        Dataset<Row> groupedDf = joinedDf.groupBy("year","month","category","sub_category")
                .agg(sum("profit_amount").as("Total Profit"),
                        sum("quantity").as("Total Quantity Sold"));

        String partOutputUrlV1 = "file:///C:\\Training\\spark_case_study_v1";
        groupedDf.write().option("header", "true").mode("Overwrite")
                .partitionBy("year","month").csv(partOutputUrlV1);

        //solution2 : using window function apply over joinedDf to get aggregated profit and quantity
        WindowSpec windowSpec = Window.partitionBy("year","month","category","sub_category");

        Dataset<Row> windowedDf = joinedDf.withColumn("Total Quantity Sold",sum(col("quantity")).over(windowSpec))
                .withColumn("Total Profit",sum(col("profit_amount")).over(windowSpec))
                .select("year","month","category","sub_category","Total Quantity Sold","Total Profit");

        String partOutputUrlV2 = "file:///C:\\Training\\spark_case_study_v2";
        windowedDf.write().option("header", "true").mode("Overwrite")
                .partitionBy("year","month").csv(partOutputUrlV2);
        
    }


}
