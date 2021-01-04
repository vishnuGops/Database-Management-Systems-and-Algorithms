import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import java.util.Arrays;
import java.util.List;

import java.lang.*;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordCount {
  static class TokenzierMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text genre = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
     
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      String year = null;
      String genre_list = null;
      String line = value.toString();
      String[] attributes = line.split(";");

      if(attributes.length >= 4) {  
        if (checkIfInteger(attributes[3]) && Integer.parseInt(attributes[3]) >= 2001 && Integer.parseInt(attributes[3]) <= 2005) {

          if (attributes[4] != null) {
            genre_list = attributes[4];
            year  = attributes[3].toString();
                       
            
            String[] tempList = genre_list.split(",");
            List<String> list = Arrays.asList(tempList);
              
              if(!list.equals("\\N") && (list.contains("Comedy")) && (list.contains("Romance"))) {
                genre.set("[2001-2005], Comedy;Romance");
                context.write(genre, one);
              } 
              else if(!list.equals("\\N") && (list.contains("Action")) && (list.contains("Thriller"))) {
                  genre.set("[2001-2005], Action;Thriller");
                  context.write(genre, one);
                }   
              else if(!list.equals("\\N") && (list.contains("Adventure")) && (list.contains("Sci-Fi"))){
                  genre.set("[2001-2005], Adventure;Sci-Fi");
                  context.write(genre, one);
                }   
            }
          }
        
        if (checkIfInteger(attributes[3]) && Integer.parseInt(attributes[3]) >= 2006 && Integer.parseInt(attributes[3]) <= 2010) {

            if (attributes[4] != null) {
              genre_list = attributes[4];
              year  = attributes[3].toString();
              
              String[] tempList = genre_list.split(",");
              List<String> list = Arrays.asList(tempList);
                
                if(!list.equals("\\N") && (list.contains("Comedy")) && (list.contains("Romance"))) {
                  genre.set("[2006-2010], Comedy;Romance");
                  context.write(genre, one);
                } 
                else if(!list.equals("\\N") && (list.contains("Action")) && (list.contains("Thriller"))) {
                    genre.set("[2006-2010], Action;Thriller");
                    context.write(genre, one);
                  }   
                else if(!list.equals("\\N") && (list.contains("Adventure")) && (list.contains("Sci-Fi"))){
                    genre.set("[2006-2010], Adventure;Sci-Fi");
                    context.write(genre, one);
                  }   
              }
            }
          
        if (checkIfInteger(attributes[3]) && Integer.parseInt(attributes[3]) >= 2011 && Integer.parseInt(attributes[3]) <= 2015) {

            if (attributes[4] != null) {
              genre_list = attributes[4];
              year  = attributes[3].toString();
              
              String[] tempList = genre_list.split(",");
              List<String> list = Arrays.asList(tempList);
                
                if(!list.equals("\\N") && (list.contains("Comedy")) && (list.contains("Romance"))) {
                  genre.set("[2011-2015], Comedy;Romance");
                  context.write(genre, one);
                } 
                else if(!list.equals("\\N") && (list.contains("Action")) && (list.contains("Thriller"))) {
                    genre.set("[2011-2015], Action;Thriller");
                    context.write(genre, one);
                  }   
                else if(!list.equals("\\N") && (list.contains("Adventure")) && (list.contains("Sci-Fi"))){
                    genre.set("[2011-2015], Adventure;Sci-Fi");
                    context.write(genre, one);
              }
            }
          }
        
      }
    }

    public boolean checkIfInteger(String input) {
      try
      {
        Integer.parseInt(input);
        return true;
      } 
      catch (Exception e) {
        return false;
      }
    } 

  }
  static class Imdbreducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

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
  Job job = Job.getInstance(conf, "Genre count");
  job.setJarByClass(WordCount.class);
  job.setMapperClass(TokenzierMapper.class);
  job.setCombinerClass(Imdbreducer.class);
  job.setReducerClass(Imdbreducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
