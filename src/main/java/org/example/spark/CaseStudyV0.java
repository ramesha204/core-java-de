package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class CaseStudyV0 {
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
        Dataset<Row> returnDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(return_url);
//        salesDf.explain();
        Dataset<Row> modifiedReturnDf = returnDf.withColumnRenamed("order id","order_id");

        Dataset<Row> modifiedSalesDf = salesDf.withColumnRenamed("order id","order_id")
                .withColumnRenamed("Sub-Category","sub_category")
                .withColumn("timestamp",to_date(col("order date"),"M/d/yyyy"))
                .withColumn("order_year",date_format(col("timestamp"),"yyyy"))
                .withColumn("order_month",date_format(col("timestamp"),"M"))
                .withColumn("order_profit",regexp_replace(col("profit"),"[$,]","").cast(DataTypes.FloatType));
//        modifiedSalesDf.show(10);
//        modifiedSalesDf.createOrReplaceTempView("order_year");
    //        Dataset<Row> modifiedDf = spark.sql("select a.order id,a.order date,a.order month,a.order year,a.profit from temp_table a");
    //        modifiedDf.show(10);
//             yearSalesDf.select("order id","order date","order year","order month","profit","timestamp").show(10);
//        yearSalesDf.explain();

       Dataset<Row> joinedDf = modifiedSalesDf.join(modifiedReturnDf,modifiedSalesDf.col("order_id")
               .equalTo(modifiedReturnDf.col("order_id")),"left_anti");
       Dataset<Row> groupedDf = joinedDf.groupBy("order_year","order_month","category","sub_category")
                       .agg(sum("order_profit").as("total_profit"),
                               sum("quantity").as("total_quantity"));
//       result1.show();
//        System.out.println( joinedDf.count());

//       WindowSpec  windowSpec = Window.partitionBy("order_year","order_month").orderBy("order_year","order_month");
       WindowSpec windowSpecAgg = Window.partitionBy("order_year","order_month","category","sub_category");

//       Dataset<Row> windowPartDf = resultDf.withColumn("row",row_number().over(windowSpec));
//       windowPartDf.show();
        Dataset<Row> windowedDf = joinedDf.withColumn("Total Quantity Sold",sum(col("quantity")).over(windowSpecAgg))
                .withColumn("Total Profit",sum(col("order_profit")).over(windowSpecAgg))
                .select("order_year","order_month","category","sub_category","Total Quantity Sold","Total Profit");

//            windowedDf.show();

        String partOutputUrlV1 = "file:///C:\\Training\\spark_case_study_v1";
        groupedDf.write().option("header", "true").mode("Overwrite")
                .partitionBy("order_year","order_month").csv(partOutputUrlV1);

        String partOutputUrlV2 = "file:///C:\\Training\\spark_case_study_v2";
        windowedDf.write().option("header", "true").mode("Overwrite")
                .partitionBy("order_year","order_month").csv(partOutputUrlV2);

//        Dataset<Row> modifiedDf = spark.sql("select to_number(a.profit, 'S$999,099.99') as amount from temp_table a");
//        modifiedDf.show();
    }


}
