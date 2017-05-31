package com.MongodbWithSpark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import com.google.common.base.Function;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;

public class GettingStarted {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws InterruptedException {
		// String outputFile = args[0];
	    /* Create the SparkSession.
	     * If config arguments are passed from the command line using --conf,
	     * parse args for the values to set.
	     */
	    SparkSession spark = SparkSession.builder()
	      .master("local")
	      .appName("MongoSparkConnectorIntro")
	      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/marketdata.minibars")
	      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/marketdata.minibars")
	      .getOrCreate();

	    // Create a JavaSparkContext using the SparkSession's SparkContext object
	    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

	   
	    /*Start Example: Read data from MongoDB************************/
	    JavaMongoRDD rdd = MongoSpark.load(jsc);
	    /*End Example**************************************************/

	    // Analyze data from MongoDB
	    System.out.println("count :"+rdd.count());
	    System.out.println(rdd.first().toString());
	    System.out.println(rdd.getNumPartitions());
	    
   
	    jsc.close();

	  }
	}