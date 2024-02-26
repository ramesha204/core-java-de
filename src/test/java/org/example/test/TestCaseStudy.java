package org.example.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.spark.CaseStudy;
import org.example.spark.Return;
import org.example.spark.Sale;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class TestCaseStudy {
    private static SparkSession spark;

    @BeforeClass
    public static void setUp() {
        spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();
    }


    @AfterClass
    public static void tearDown() {
        spark.stop();
    }


    @Test
    public void testCreateReportSolution1() {
        List<Return> returnList = Arrays.asList(
                new Return("CA-2012-SA20830140-41210", "Yes"),
                new Return("IN-2012-PB19210127-41259", "Yes"),
                new Return("CA-2012-SC20095140-41174", "Yes"),
                new Return("CA-2014-AB10015140-41954", "Yes")
        );

        List<Sale> saleList = Arrays.asList(
                new Sale("CA-2014-AB10015140-41954", "11/11/2014", "Technology", "Phones", "$62.15", "2","Yes"),
                new Sale("CA-2014-AB10015140-41900", "11/11/2014", "Technology", "Phones", "$72.15", "2","No"),
                new Sale("IN-2014-JR162107-41675", "2/5/2014", "Furniture", "Chairs", "-$288.77", "7","No"),
                new Sale("IN-2014-JR162107-41675", "2/5/2014", "Furniture", "Chairs", "$388.77", "10","No")
        );
        Dataset<Row> salesDf = spark.createDataset(saleList, Encoders.bean(Sale.class)).toDF();
        Dataset<Row> returnsDf = spark.createDataset(returnList, Encoders.bean(Return.class)).toDF();
        Dataset<Row> resultDf = CaseStudy.createReportSolution1(salesDf, returnsDf);
         Assert.assertFalse(resultDf.isEmpty());
         assertEquals(2,resultDf.count());

    }


    @Test
    public void testCreateReportSolution2() {
        List<Return> returnList = Arrays.asList(
                new Return("CA-2012-SA20830140-41210", "Yes"),
                new Return("IN-2012-PB19210127-41259", "Yes"),
                new Return("CA-2012-SC20095140-41174", "Yes"),
                new Return("CA-2014-AB10015140-41954", "Yes")
        );

        List<Sale> saleList = Arrays.asList(
                new Sale("CA-2014-AB10015140-41954", "11/11/2014", "Technology", "Phones", "$62.15", "2","Yes"),
                new Sale("CA-2014-AB10015140-41900", "11/11/2014", "Technology", "Phones", "$72.15", "2","No"),
                new Sale("IN-2014-JR162107-41675", "2/5/2014", "Furniture", "Chairs", "-$288.77", "7","No"),
                new Sale("IN-2014-JR162107-41675", "2/5/2014", "Furniture", "Chairs", "$388.77", "10","No")
        );
        Dataset<Row> salesDf = spark.createDataset(saleList, Encoders.bean(Sale.class)).toDF();
        Dataset<Row> returnsDf = spark.createDataset(returnList, Encoders.bean(Return.class)).toDF();
        Dataset<Row> resultDf = CaseStudy.createReportSolution2(salesDf, returnsDf);
        Assert.assertFalse(resultDf.isEmpty());

    }


}
