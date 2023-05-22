package org.apache.hadoop.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import org.json.JSONObject;
import java.util.List;
import java.util.Arrays;

public class PalindromeCheckSpark {
    
    public static boolean isPalindrome(String str) {
        return str.equals(new StringBuilder(str).reverse().toString());
    }

    public static boolean isWord(String str){
        return (str.length() > 2 ? true : false);
    }
    
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PalindromeCheckSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        String inputPath = args[0];
        String outputPath = args[1];
        
        JavaRDD<String> inputRDD = sc.textFile(inputPath);
        
        JavaRDD<String> cleanedRDD = inputRDD.map(line -> {
            JSONObject json = new JSONObject(line);
            String desiredKey = json.getString("reviewText");
            return desiredKey.replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", "").toLowerCase();
        });
        
        JavaRDD<String> palindromeRDD = cleanedRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        
        JavaPairRDD<String, Integer> onesRDD = palindromeRDD.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts = onesRDD.filter(s->PalindromeCheckSpark.isWord(s._1) && PalindromeCheckSpark.isPalindrome(s._1)).sortByKey(true).reduceByKey((i1, i2) -> i1 + i2);
        List<Tuple2<String, Integer>> output = counts.collect();
        counts.saveAsTextFile(outputPath);
    }
}
