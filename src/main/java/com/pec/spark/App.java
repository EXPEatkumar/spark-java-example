package com.pec.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Starter
 */
public class App {

    public static void main(String[] args) throws Exception {

        // creates the spark session.
        final SparkSession session = SparkSession
                .builder()
                .appName("spark-java-example")
                .master("local[3]")
                .getOrCreate();

        // java context
        final JavaSparkContext javaSparkContext = new JavaSparkContext(session.sparkContext());
        // the data to be processed
        final List<Integer> integers = Arrays.asList(1, 2,45, 25, 23534, 3465, 45,32534);
        // rdd creation
        final JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(integers, 3);

        // processing on java rdd
        javaRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println("Java RDD : " + integer);
                Thread.sleep(3000);
            }
        });

        // delay
        Thread.sleep(100000);
        // session close.
        javaSparkContext.stop();
    }
}
