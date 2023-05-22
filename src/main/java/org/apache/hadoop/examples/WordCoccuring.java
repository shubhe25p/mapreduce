package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;

public class WordCoccuring {

  public static List<String> splitIntoSentences(String paragraph) {
        List<String> sentences = new ArrayList<>();

        // Split the paragraph into sentences using a regex pattern
        String pattern = "([^.!?]+[.!?])";
        Pattern sentencePattern = Pattern.compile(pattern);
        Matcher matcher = sentencePattern.matcher(paragraph);

        while (matcher.find()) {
            String sentence = matcher.group(0).trim();
            sentences.add(sentence);
        }

        return sentences;
    }

  public static List<String[]> findCoOccurringWordPairs(List<String> sentences) {
        List<String[]> wordPairs = new ArrayList<>();
        HashSet<String[]> wordPairSet = new HashSet<String[]>();
        for (String sentence : sentences) {
            String[] words = sentence.split("\\s+");

            for (int i = 0; i < words.length - 1; i++) {
                for (int j = i + 1; j < words.length; j++) {
                    String word1 = words[i];
                    String word2 = words[j];

                    wordPairSet.add(new String[]{word1, word2});
                }
            }
            for(String[] obj: wordPairSet)
                wordPairs.add(obj);
        }

        return wordPairs;
    }

  public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      String jsonString = value.toString();
      JSONObject json = new JSONObject(jsonString);

      // Extract the desired key from the JSON object
      String desiredKey = json.getString("reviewText");
      List<String[]> wordPairs = WordCoccuring.findCoOccurringWordPairs(WordCoccuring.splitIntoSentences(desiredKey.toString()));
      for(String[] pair: wordPairs){
        pair[0].replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", "");
        pair[1].replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", "");
        String str = pair[0]+"-"+pair[1];
        word.set(str);
        context.write(word, one);
        }
      }
    }

  public static class IntSumReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "WordCoccuring");
    job.setJarByClass(WordCoccuring.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
