package cn.edu.seu.cloud.jn2;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

public class Main {

    //public static String HDFS = "hdfs://";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]|:{2}");


    public static void main(String[] args) throws Exception {
        Map<String, String> path = new HashMap<String, String>();

        if (args.length != 3) {
            System.err.println("Usage: hadoop jar DataSpark_problem2.jar cn.edu.seu.cloud.jn2.Main <training_path> <test_path> <output_path>");
            System.exit(2);
        } 
//        else if (args.length == 4) {
//        	HDFS = HDFS + args[3];
//        } else {
//        	HDFS = HDFS + "namenode:9000";
//        }
        
        String middle_path = "/user/dataspark/recommmend";

        path.put("Step1Input", args[0]);
        path.put("Step1Output", middle_path + "/stage1");
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", middle_path + "/stage2");
        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Output1", middle_path + "/stage3");
        path.put("Step3Input2", path.get("Step2Output"));
        path.put("Step3Output2", middle_path + "/stage4");
		path.put("Step4Input1", path.get("Step3Output1"));
		path.put("Step4Input2", path.get("Step3Output2"));
		path.put("Step4Output", middle_path + "/stage5");
		path.put("Step5Input", path.get("Step4Output"));
		path.put("Step5Output", args[2]);


        Stage1_uv.run(path);
        Stage2_cc.run(path);
        Stage3_uvs.run1(path);
        Stage3_uvs.run2(path);
        Stage4_mul_add.run(path);
        Stage4_mul_add.run2(path);

        System.exit(0);
    }

    public static JobConf config() {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("Film_Recommend");
        conf.set("mapreduce.task.io.sort.mb", "1024");
        return conf;
    }

}