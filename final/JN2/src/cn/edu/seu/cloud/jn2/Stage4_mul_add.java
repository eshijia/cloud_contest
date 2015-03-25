package cn.edu.seu.cloud.jn2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.edu.seu.cloud.jn2.HDFS_Controller;

public class Stage4_mul_add {
	
    public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Main.DELIMITER.split(values.toString());

            if (flag.equals("stage4")) {
                String[] v1 = tokens[0].split("@");
                String itemID1 = v1[0];
                String itemID2 = v1[1];
                String num = tokens[1];

                Text k = new Text(itemID1);
                Text v = new Text("A:" + itemID2 + "," + num);

                context.write(k, v);
            } else if (flag.equals("stage3")) {
                String[] v2 = tokens[1].split("@");
                String itemID = tokens[0];
                String userID = v2[0];
                String pref = v2[1];
                Text k = new Text(itemID);
                Text v = new Text("B:" + userID + "," + pref);

                context.write(k, v);
            }
        }

    }

    public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString() + ":");

            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();

            for (Text line : values) {
                String val = line.toString();
                System.out.println(val);

                if (val.startsWith("A:")) {
                    String[] kv = Main.DELIMITER.split(val.substring(2));
                    mapA.put(kv[0], kv[1]);
                    

                } else if (val.startsWith("B:")) {
                    String[] kv = Main.DELIMITER.split(val.substring(2));
                    mapB.put(kv[0], kv[1]);
                }
            }
            
            

            double result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();

                int num = Integer.parseInt(mapA.get(mapk));
                Iterator<String> iterb = mapB.keySet().iterator();
                while (iterb.hasNext()) {
                    String mapkb = iterb.next();

                    double pref = Double.parseDouble(mapB.get(mapkb));
                    result = num * pref;

                    Text k = new Text(mapkb);
                    Text v = new Text(mapk + "," + result + "," + key.toString() + "," + num);
                    context.write(k, v);
                    
                }
            }
        }
    }
    
    public static class Step4_RecommendMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Main.DELIMITER.split(values.toString());
            Text k = new Text(tokens[0]);
            Text v = new Text(tokens[1] + "," + tokens[2] + "," + tokens[3] + "," + tokens[4]);
            context.write(k, v);
        }
    }

    public static class Step4_RecommendReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString() + ":");
            Map<String, Double> map = new HashMap<String, Double>();
            Map<String, Integer> single_mov = new HashMap<String, Integer>();
            Map<String, Integer> para = new HashMap<String, Integer>();
            
            for (Text line : values) {
                System.out.println(line.toString());
                String[] tokens = Main.DELIMITER.split(line.toString());
                String itemID = tokens[0];
                Double score = Double.parseDouble(tokens[1]);
                single_mov.put(itemID + ":" + tokens[2], Integer.parseInt(tokens[3])); 
                if (map.containsKey(itemID)) {
                     map.put(itemID, map.get(itemID) + score);
                } else {
                     map.put(itemID, score);
                }
                if (para.containsKey(itemID)) {
                	para.put(itemID, para.get(itemID) + Integer.parseInt(tokens[3]));
                } else {
                	para.put(itemID, Integer.parseInt(tokens[3]));
                }
            }
            
            Iterator<String> iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String itemID = iter.next();
	            double source_score = map.get(itemID);
	            double score = Math.ceil( Math.round(source_score/para.get(itemID) * 10) / 10.0 * 2) / 2;   
	            Text v = new Text(itemID + ":" + score);
	            if (!single_mov.containsKey(itemID + ":" + itemID)) {
	                context.write(key, v);
	            }
            }
        }
    }

    public static void run2(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Main.config();
        conf.set("mapred.textoutputformat.separator", ":");

        String input = path.get("Step5Input");
        String output = path.get("Step5Output");

        HDFS_Controller hdfs = new HDFS_Controller(conf);
        hdfs.rmr(output);

        @SuppressWarnings("deprecation")
		Job job = new Job(conf);
        job.setJarByClass(Stage4_mul_add.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Stage4_mul_add.Step4_RecommendMapper.class);
        job.setReducerClass(Stage4_mul_add.Step4_RecommendReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        System.out.println(output);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }


    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Main.config();

        String input1 = path.get("Step4Input1");
        String input2 = path.get("Step4Input2");
        String output = path.get("Step4Output");

        HDFS_Controller hdfs = new HDFS_Controller(conf);
        hdfs.rmr(output);

        @SuppressWarnings("deprecation")
		Job job = new Job(conf);
        job.setJarByClass(Stage4_mul_add.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Stage4_mul_add.Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Stage4_mul_add.Step4_AggregateReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}