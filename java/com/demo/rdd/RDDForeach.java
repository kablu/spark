package com.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class RDDForeach {

	public static void main(String[] args) {
		
		String appName = "RDD_APP";
		
		String appMaster = "local[2]";
		
		JavaSparkContext javaSparkContext = null;
		
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(appMaster);
		
		conf.set("spark.testing.memory", "2147480000");
		
		javaSparkContext = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = javaSparkContext.textFile("data/movietweets.csv");
		
		lines.foreach(new VoidFunction<String>() {
			
			@Override
			public void call(String l) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("**" + l);
			}
		});
	}
}
