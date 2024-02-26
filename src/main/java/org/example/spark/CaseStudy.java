package org.example.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class CaseStudy {

    static String default_sales_url = "file:///C:\\Training\\sockgen_spark_java\\dataset\\Global Superstore Sales - Global Superstore Sales.csv";
    static String default_return_url = "file:///C:\\Training\\sockgen_spark_java\\dataset\\Global Superstore Sales - Global Superstore Returns.csv";
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        System.out.println("Enter the input directory for reading the  Global Superstore Sales csv");

        String sales_url= sc.nextLine();
                sales_url = sales_url.isEmpty() ? default_sales_url : sales_url;

        System.out.println("Enter the input directory for reading the Global Superstore Returns csv");
        String return_url= sc.nextLine();
        return_url = return_url.isEmpty() ? default_return_url : return_url;

        System.out.println("Started creating  a report that contains the aggregate (sum) Profit and Quantity, based on Year, Month, Category, Sub Category");

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
        Dataset<Row> modifiedReturnsDf = returnsDf.withColumnRenamed("order id","orderId");

        Dataset<Row> modifiedSalesDf = salesDf.withColumnRenamed("order id","orderId")
                .withColumnRenamed("order date","orderDate")
                .withColumnRenamed("Sub-Category","subCategory");

        createReportSolution1(modifiedSalesDf,modifiedReturnsDf);
        createReportSolution2(modifiedSalesDf,modifiedReturnsDf);

    }


    //solution1 : using groupBy on specified columns to get aggregated profit and quantity
    public static  Dataset<Row> createReportSolution1(Dataset<Row> salesDf,Dataset<Row> modifiedReturnsDf){
        Dataset<Row> modifiedSalesDf = salesDf
                .withColumn("year",year(to_date(col("orderDate"),"M/d/yyyy")))
                .withColumn("month",month(to_date(col("orderDate"),"M/d/yyyy")))
                .withColumn("profit_amount",regexp_replace(col("profit"),"[$,]","").cast(DataTypes.FloatType));

        Dataset<Row> joinedDf = modifiedSalesDf.join(modifiedReturnsDf,modifiedSalesDf.col("orderId")
                .equalTo(modifiedReturnsDf.col("orderId"))
                .and(modifiedSalesDf.col("Returns").equalTo(modifiedReturnsDf.col("Returned"))),"left_anti");

        Dataset<Row> groupedDf = joinedDf.groupBy("year","month","category","subCategory")
                .agg(sum("profit_amount").as("Total Profit"),
                        sum("quantity").as("Total Quantity Sold"));

        String partOutputUrlV1 = "file:///C:\\Training\\spark_case_study_v1";
        groupedDf.write().option("header", "true").mode("Overwrite")
                .partitionBy("year","month").csv(partOutputUrlV1);
    return groupedDf;
    }

    //solution2 : using window function apply over joinedDf to get aggregated profit and quantity
    public static  Dataset<Row> createReportSolution2(Dataset<Row> salesDf,Dataset<Row> modifiedReturnsDf){
        Dataset<Row> modifiedSalesDf = salesDf
                .withColumn("year",year(to_date(col("orderDate"),"M/d/yyyy")))
                .withColumn("month",month(to_date(col("orderDate"),"M/d/yyyy")))
                .withColumn("profit_amount",regexp_replace(col("profit"),"[$,]","").cast(DataTypes.FloatType));

        Dataset<Row> joinedDf = modifiedSalesDf.join(modifiedReturnsDf,modifiedSalesDf.col("orderId")
                .equalTo(modifiedReturnsDf.col("orderId"))
                .and(modifiedSalesDf.col("Returns").equalTo(modifiedReturnsDf.col("Returned"))),"left_anti");

        WindowSpec windowSpec = Window.partitionBy("year","month","category","subCategory");

        Dataset<Row> windowedDf = joinedDf.withColumn("Total Quantity Sold",sum(col("quantity")).over(windowSpec))
                .withColumn("Total Profit",sum(col("profit_amount")).over(windowSpec))
                .select("year","month","category","subCategory","Total Quantity Sold","Total Profit");

        String partOutputUrlV2 = "file:///C:\\Training\\spark_case_study_v2";
        windowedDf.write().option("header", "true").mode("Overwrite")
                .partitionBy("year","month").csv(partOutputUrlV2);
        return windowedDf;
    }

}
