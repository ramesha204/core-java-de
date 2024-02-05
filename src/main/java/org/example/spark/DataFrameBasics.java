package org.example.spark;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class DataFrameBasics {
    static String url = "file:///C:\\Training\\sockgen_spark_java\\salary.csv";


    public static void rddVersion(SparkSession spark) {
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> lines = sparkContext.textFile(url)
                .filter(line -> !line.startsWith("name")); //to remove header

        JavaRDD<Person> people = lines.map(new Function<String, Person>() {
            @Override
            public Person call(String s) throws Exception {
                String[] values = s.split(",");
                String name = values[0];
                Integer age = Integer.parseInt(values[1]);
                Integer salary = Integer.parseInt(values[2]);
                String designation = values[3];
                Person p = new Person();
                p.setAge(age);
                p.setName(name);
                p.setSalary(salary);
                p.setDesignation(designation);
                return p;
            }
        });

        people.filter((Function<Person, Boolean>) person -> person.getAge() > 25)
                .collect().forEach(System.out::println);

    }

    public static void dataframeVersion(SparkSession spark) {

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(url);

        df.show();
        df.printSchema();
        df.filter("age > 25").show();
        df.filter("age > 25").select("name","designation").show();
        df.selectExpr("name","age","salary/80 as dollar_salary").show();
        Dataset<Row> dfGBP = df.withColumn("GBP_salary",df.col("salary").divide(90));
//        dfGBP.show();
        Dataset<Row> dfModified = dfGBP.drop("age").withColumnRenamed("designation","company_designation");
        dfGBP.show();
        dfModified.show(); //datset are immutable objects a drop doesn't drop the existing dataset , it will create new dataset
        Dataset<Row> dfModifiedGBP = df.withColumn("GBP_Salary", df.col("salary").divide(80))
                .drop("age")
                .withColumnRenamed("designation", "company_designation")
                .dropDuplicates();

        dfModifiedGBP.show(); //when we encounter action then only all the sequence of transformations and materialize the dataset

        dfModifiedGBP.explain();
    }

    public static void datasetVersion(SparkSession spark) {

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(url);
//        df.show();
//        df.printSchema();
//        df.filter("age > 25").show();

        Dataset<Person> x = df.as(Encoders.bean(Person.class));
        Dataset<Person> filteredDs = x.filter(new FilterFunction<Person>() {
            @Override
            public boolean call(Person value) throws Exception {
                return value.getAge() < 25;
            }
        });
        filteredDs.show();

        Dataset<Person> adult = x.filter((FilterFunction<Person>) p -> p.isAdult());

        adult.show();
    }

    public static Dataset<Person> personsAge(Dataset<Row> df) {

        Dataset<Person> x = df.as(Encoders.bean(Person.class));
        Dataset<Person> filteredDs = x.filter(new FilterFunction<Person>() {
            @Override
            public boolean call(Person value) throws Exception {
                return value.getAge() < 25;
            }
        });
        filteredDs.show();

        Dataset<Person> adult = x.filter((FilterFunction<Person>) p -> p.isAdult());

        return  adult;
    }

    public static void sqlVersion(SparkSession spark) throws AnalysisException {

        Dataset<Row> df = spark.read()
                .option("inferSchema", "true")
                .option("header", "true").csv(url);
        df.createOrReplaceTempView("emptable");

//        spark.sql("select name,age from emptable where age > 25").show();

    }



    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();
        sparkOperations(spark);
 //       rddVersion(spark);
       //dataframeVersion(spark);
        //datasetVersion(spark);
//        try {
//           sqlVersion(spark);
//        } catch (AnalysisException e) {
//            e.printStackTrace();
//        }
    }

    static String productUrl = "file:///C:\\Training\\sockgen_spark_java\\product_meta.csv";
    static String salesUrl = "file:///C:\\Training\\sockgen_spark_java\\sales_1.csv";
    static String sales2Url = "file:///C:\\Training\\sockgen_spark_java\\sales_2.csv";
    public static void sparkOperations(SparkSession spark) {
        Dataset<Row> productDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(productUrl);
        //productDf.show();
        Dataset<Row> salesDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(salesUrl);
        //salesDf.show();
        Dataset<Row> sales2Df = readSales(spark, sales2Url);
        //sales2Df.show();
        //sales2Df.printSchema();
        Dataset<Row> castedDf = salesDf
                .withColumn("date_casted", salesDf.col("date_of_sale").cast(DataTypes.DateType))
                .withColumn("unit_price_casted", salesDf.col("date_of_sale").cast(DataTypes.FloatType));
        salesDf = castedDf
                .drop("date_of_sale").withColumnRenamed("date_casted", "date_of_sale")
                .drop("unit_price").withColumnRenamed("unit_price_casted", "unit_price");
        //salesDf.printSchema();
        Dataset<Row> allSalesDf = salesDf.select("item_id", "item_qty", "unit_price", "total_amount", "date_of_sale")
                .union(sales2Df.select("item_id", "item_qty", "unit_price", "total_amount", "date_of_sale"));
        //allSalesDf.show();
        Dataset<Row> joinedDf = salesDf.join(productDf,
                salesDf.col("item_id").equalTo(productDf.col("item_id")),
                "inner");

        //where("item_id is null");
        Dataset<Row> outerjoinedDf = salesDf.join(productDf,
                salesDf.col("item_id").equalTo(productDf.col("item_id")),
                "right_outer").where(salesDf.col("item_id").isNull());

        outerjoinedDf.show();


        //where("item_id is null");
        Dataset<Row> leftOuterDf = productDf.join(salesDf,
                salesDf.col("item_id").equalTo(productDf.col("item_id")),
                "right_outer").where(salesDf.col("item_id").isNotNull());

        leftOuterDf.show();

        //where("item_id is null");
        Dataset<Row> leftSemiDf = productDf.join(salesDf,
                salesDf.col("item_id").equalTo(productDf.col("item_id")),
                "left_semi");

        leftSemiDf.show();



        //with SQL
        salesDf.createOrReplaceTempView("sales");
        productDf.createOrReplaceTempView("product");
        Dataset<Row> temp = spark.sql("select * from sales a join product b " +
                "on a.item_id = b.item_id");

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static Dataset<Row> readSales(SparkSession spark,String filePath){
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