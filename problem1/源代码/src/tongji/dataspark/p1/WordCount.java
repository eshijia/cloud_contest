package tongji.dataspark.p1;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
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

public class WordCount {

  public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable> {
    
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String punc = "$`~!@#%^&*()[]{}:;\"',.<>?\\-+=";

    public void map(Object key, Text value, Context context) 
      throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        for (String str : tokens) {
        	if (!str.equals("")) {
        		String target = StringUtils.strip(str, punc).toLowerCase(); 
        		if (!target.equals("") && ( (target.charAt(0) >='a' && target.charAt(0) <= 'z') || (target.charAt(0) >= 'A' && target.charAt(0) <='Z') )) {
        			word.set(target);
        			context.write(word, one);	
        		}
        	}
        }      
    }
  }
	
  public static class IntSumReducer 
    extends Reducer<Text,IntWritable,Text,IntWritable> {
        
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
    if (otherArgs.length != 2) {
        System.err.println("Usage: hadoop jar Dataspark_problem1.jar <in> <out>");
        System.exit(2);
    }
    conf.set("mapred.textoutputformat.separator", ":");
        
    @SuppressWarnings("deprecation")
	Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
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