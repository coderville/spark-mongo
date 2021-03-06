package com.MongodbWithSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class WordCountTask {
	 private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTask.class);

	  public static void main(String[] args) {
	    checkArgument(args.length > 1, "Please provide the path of input file as first parameter.");
	    System.out.println("inputFilePath : "+ args[0]);
	    System.out.println("outputFilePath : "+ args[1]);
	    new WordCountTask().run(args[0],args[1]);
	  }

  public void run(String inputFilePath,String outputFilePath) {
	  
	  
    String master = "spark://192.168.1.2:7077";

    SparkConf conf = new SparkConf()
            .setAppName(WordCountTask.class.getName())
            .setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);

        context.textFile(inputFilePath)
            .flatMap(text -> Arrays.asList(text.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((a, b) -> a + b).saveAsTextFile(outputFilePath);
          //  .foreach(result -> LOGGER.info(
          //      String.format("Word [%s] count [%d].", result._1(), result._2)));
  }
}