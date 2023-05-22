package org.apache.hadoop.examples;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONObject;
import org.apache.spark.api.java.JavaPairRDD;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import scala.Tuple2;

public class WordCoccuringSpark {

    public static List<String> splitIntoSentences(String paragraph) {
        List<String> sentences = new ArrayList<>();

        // Split the paragraph into sentences using a regex pattern
        String pattern = "([^.!?]+[.!?])";
        String[] sentenceArray = paragraph.split(pattern);
        sentences.addAll(Arrays.asList(sentenceArray));

        return sentences;
    }

    public static String convertToString(String[] str){
        return str[0]+"-"+str[1];
    }

    public static List<String[]> findCoOccurringWordPairs(List<String> sentences) {
        List<String[]> wordPairs = new ArrayList<>();

        for (String sentence : sentences) {
            String[] words = sentence.split("\\s+");

            for (int i = 0; i < words.length - 1; i++) {
                for (int j = i + 1; j < words.length; j++) {
                    String word1 = words[i].replaceAll("[^a-zA-Z0-9\\s]+", "");
                    String word2 = words[j].replaceAll("[^a-zA-Z0-9\\s]+", "");

                    wordPairs.add(new String[]{word1, word2});
                }
            }
        }

        return wordPairs;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCoccuringSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Input and output paths
        String inputPath = args[0];
        String outputPath = args[1];

        // Load the input text file
        JavaRDD<String> inputRDD = sc.textFile(inputPath);

        // Map phase
        JavaRDD<String[]> wordPairRDD = inputRDD.map(line -> {
            JSONObject json = new JSONObject(line);
            String desiredKey = json.getString("reviewText");
            List<String> sentences = WordCoccuringSpark.splitIntoSentences(desiredKey);
            return WordCoccuringSpark.findCoOccurringWordPairs(sentences);
        }).flatMap(list -> list.iterator());

        // Reduce phase
        JavaPairRDD<String, Integer> onesRDD = wordPairRDD.mapToPair(pair -> new Tuple2<>(WordCoccuringSpark.convertToString(pair), 1));
        JavaPairRDD<String, Integer> counts = onesRDD.sortByKey(true).reduceByKey((i1, i2) -> i1 + i2);
        List<Tuple2<String, Integer>> output = counts.collect();
        counts.saveAsTextFile(outputPath);
    }
}
