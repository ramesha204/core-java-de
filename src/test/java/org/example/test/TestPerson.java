package org.example.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.example.spark.DataFrameBasics;
import org.example.spark.Person;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;


public class TestPerson {
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
    public void testUniqueStores() {
        List<Person> personList = Arrays.asList(
                new Person(10,"Ram",20000,"SE1"),
                new Person(26,"Sita",21000,"SE1")
                );
        Dataset<Row> personDf = spark.createDataset(personList, Encoders.bean(Person.class)).toDF();
        Dataset<Person> adult = DataFrameBasics.personsAge(personDf);
        Assert.assertFalse(adult.isEmpty());

    }
}
